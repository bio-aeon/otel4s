/*
 * Copyright 2024 Typelevel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.typelevel.otel4s.sdk.metrics.aggregation

import cats.Monad
import cats.data.NonEmptyVector
import cats.effect.{Concurrent, Ref}
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.metrics.MeasurementValue
import org.typelevel.otel4s.sdk.TelemetryResource
import org.typelevel.otel4s.sdk.common.InstrumentationScope
import org.typelevel.otel4s.sdk.context.Context
import org.typelevel.otel4s.sdk.metrics.data.{
  AggregationTemporality,
  ExemplarData,
  MetricData,
  MetricPoints,
  PointData,
  TimeWindow
}
import org.typelevel.otel4s.sdk.metrics.exemplar.{ExemplarReservoir, Reservoirs}
import org.typelevel.otel4s.sdk.metrics.internal.MetricDescriptor

private final class ExponentialHistogramAggregator[
    F[_]: Concurrent,
    A: MeasurementValue
](
    reservoirs: Reservoirs[F],
    reservoirSize: Int,
    maxBuckets: Int,
    maxScale: Int
) extends Aggregator.Synchronous[F, A] {
  import ExponentialHistogramAggregator._

  type Point = PointData.ExponentialHistogram

  def createAccumulator: F[Aggregator.Accumulator[F, A, Point]] =
    for {
      state <- Concurrent[F].ref(emptyState(maxScale))
      reservoir <- reservoirs.fixedSize(reservoirSize)
    } yield new Accumulator(state, maxBuckets, maxScale, reservoir)

  def toMetricData(
      resource: TelemetryResource,
      scope: InstrumentationScope,
      descriptor: MetricDescriptor,
      points: NonEmptyVector[Point],
      temporality: AggregationTemporality
  ): F[MetricData] =
    Concurrent[F].pure(
      MetricData(
        resource,
        scope,
        descriptor.name.toString,
        descriptor.description,
        descriptor.sourceInstrument.unit,
        MetricPoints.exponentialHistogram(points, temporality)
      )
    )

}

private object ExponentialHistogramAggregator {

  /** Creates a histogram aggregation with the given values.
    *
    * @param reservoirs
    *   the allocator of exemplar reservoirs
    *
    * @param reservoirSize
    *   the maximum number of exemplars to preserve
    *
    * @param maxBuckets
    *   the max number of positive and negative buckets
    *
    * @param maxScale
    *   the maximum and initial scale
    *
    * @tparam F
    *   the higher-kinded type of a polymorphic effect
    *
    * @tparam A
    *   the type of the values to record
    */
  def apply[F[_]: Concurrent, A: MeasurementValue: Numeric](
      reservoirs: Reservoirs[F],
      reservoirSize: Int,
      maxBuckets: Int,
      maxScale: Int
  ): Aggregator.Synchronous[F, A] =
    new ExponentialHistogramAggregator[F, A](
      reservoirs,
      reservoirSize,
      maxBuckets,
      maxScale
    )

  private final case class State(
      sum: Double,
      min: Double,
      max: Double,
      count: Long,
      zeroCount: Long,
      currentScale: Int,
      positiveBuckets: Buckets,
      negativeBuckets: Buckets
  )

  private final class Buckets(
      scale: Int,
      indexStart: Int,
      indexEnd: Int,
      buckets: Vector[Long]
  ) {
    def scale: Int
    def offset: Int
    def counts: Vector[Long]
    def totalCount: Long

    /** _updateBuckets maps the incoming value to a bucket index for the current
      * scale. If the bucket index is outside of the range of the backing array,
      * it will rescale the backing array and update the mapping for the new
      * scale.
      */
    def record(value: Double): Buckets = {
      require(value != 0.0, "value must be positive")
      ???
    }

  }

  private def emptyState(maxScale: Int): State =
    State(
      sum = 0,
      min = Double.MaxValue,
      max = Double.MinValue,
      count = 0L,
      zeroCount = 0L,
      currentScale = maxScale,
      positiveBuckets = new Buckets(maxScale),
      negativeBuckets = new Buckets(maxScale)
    )

  private final class Accumulator[F[_]: Monad, A: MeasurementValue](
      stateRef: Ref[F, State],
      maxBuckets: Int,
      maxScale: Int,
      reservoir: ExemplarReservoir[F, A]
  ) extends Aggregator.Accumulator[F, A, PointData.ExponentialHistogram] {

    private val toDouble: A => Double =
      MeasurementValue[A] match {
        case MeasurementValue.LongMeasurementValue(cast) =>
          cast.andThen(_.toDouble)
        case MeasurementValue.DoubleMeasurementValue(cast) =>
          cast
      }

    def aggregate(
        timeWindow: TimeWindow,
        attributes: Attributes,
        reset: Boolean
    ): F[Option[PointData.ExponentialHistogram]] =
      reservoir.collectAndReset(attributes).flatMap { rawExemplars =>
        stateRef.modify { state =>
          val exemplars = rawExemplars.map { e =>
            ExemplarData.double(
              e.filteredAttributes,
              e.timestamp,
              e.traceContext,
              toDouble(e.value)
            )
          }

          val stats = Option.when(state.count > 0) {
            PointData.ExponentialHistogram.Stats(
              state.sum,
              state.min,
              state.max,
              state.zeroCount,
              state.count
            )
          }

          def makeBuckets(buckets: Buckets) =
            PointData.ExponentialHistogram.Buckets(
              buckets.scale,
              buckets.offset,
              buckets.counts,
              buckets.totalCount
            )

          val histogram = PointData.exponentialHistogram(
            timeWindow = timeWindow,
            attributes = attributes,
            exemplars = exemplars,
            scale = state.currentScale,
            stats = stats,
            positiveBuckets = makeBuckets(state.positiveBuckets),
            negativeBuckets = makeBuckets(state.negativeBuckets)
          )

          val next = if (reset) emptyState(maxScale) else state

          (next, Some(histogram))
        }
      }

    def record(value: A, attributes: Attributes, context: Context): F[Unit] = {
      val doubleValue = toDouble(value)

      (reservoir.offer(value, attributes, context) >> stateRef.update { state =>
        val c = doubleValue.compare(0.0)

        val zeroCount =
          if (c == 0) state.zeroCount + 1 else state.zeroCount

        val positive =
          if (c > 0) state.positiveBuckets.record(doubleValue)
          else state.positiveBuckets

        val negative =
          if (c < 0) state.negativeBuckets.record(-doubleValue)
          else state.negativeBuckets

        state.copy(
          sum = state.sum + doubleValue,
          min = math.min(state.min, doubleValue),
          max = math.max(state.max, doubleValue),
          count = state.count + 1,
          zeroCount = zeroCount,
          positiveBuckets = positive,
          negativeBuckets = negative
        )
      }).whenA(doubleValue.isFinite)
    }
  }

}
