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

package org.typelevel.otel4s.sdk.metrics

import cats.data.NonEmptyList
import cats.effect.Clock
import cats.effect.MonadCancelThrow
import cats.effect.Resource
import cats.effect.std.Console
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import org.typelevel.ci.CIString
import org.typelevel.otel4s.metrics.Measurement
import org.typelevel.otel4s.metrics.MeasurementValue
import org.typelevel.otel4s.metrics.ObservableCounter
import org.typelevel.otel4s.metrics.ObservableMeasurement
import org.typelevel.otel4s.sdk.context.AskContext
import org.typelevel.otel4s.sdk.metrics.internal.CallbackRegistration
import org.typelevel.otel4s.sdk.metrics.internal.InstrumentDescriptor
import org.typelevel.otel4s.sdk.metrics.internal.MeterSharedState

/** An asynchronous instrument that reports monotonically increasing value(s) when the instrument is being observed.
  *
  * @see
  *   [[https://opentelemetry.io/docs/specs/otel/metrics/api/#asynchronous-counter]]
  */
private object SdkObservableCounter {

  final case class Builder[
      F[_]: MonadCancelThrow: Clock: Console: AskContext,
      A: MeasurementValue
  ](
      name: String,
      sharedState: MeterSharedState[F],
      unit: Option[String] = None,
      description: Option[String] = None
  ) extends ObservableCounter.Builder[F, A] {

    def withUnit(unit: String): ObservableCounter.Builder[F, A] =
      copy(unit = Some(unit))

    def withDescription(description: String): ObservableCounter.Builder[F, A] =
      copy(description = Some(description))

    def createWithCallback(
        cb: ObservableMeasurement[F, A] => F[Unit]
    ): Resource[F, ObservableCounter] = {
      val descriptor = makeDescriptor

      val makeCallbackRegistration: F[CallbackRegistration[F]] =
        MeasurementValue[A] match {
          case MeasurementValue.LongMeasurementValue(cast) =>
            sharedState
              .registerObservableMeasurement[Long](descriptor)
              .map { observable =>
                val callback = cb { (value, attributes) =>
                  observable.record(cast(value), attributes)
                }

                new CallbackRegistration[F](
                  NonEmptyList.one(observable),
                  callback
                )
              }

          case MeasurementValue.DoubleMeasurementValue(cast) =>
            sharedState
              .registerObservableMeasurement[Double](descriptor)
              .map { observable =>
                val callback = cb { (value, attributes) =>
                  observable.record(cast(value), attributes)
                }

                new CallbackRegistration[F](
                  NonEmptyList.one(observable),
                  callback
                )
              }
        }

      for {
        cr <- Resource.eval(makeCallbackRegistration)
        _ <- sharedState.withCallback(cr)
      } yield new ObservableCounter {}
    }

    def create(
        measurements: F[Iterable[Measurement[A]]]
    ): Resource[F, ObservableCounter] =
      createWithCallback { cb =>
        for {
          m <- measurements
          _ <- m.toVector.traverse_(m => cb.record(m.value, m.attributes))
        } yield ()
      }

    def createObserver: F[ObservableMeasurement[F, A]] =
      MeasurementValue[A] match {
        case MeasurementValue.LongMeasurementValue(cast) =>
          sharedState
            .registerObservableMeasurement[Long](makeDescriptor)
            .map(_.contramap(cast))

        case MeasurementValue.DoubleMeasurementValue(cast) =>
          sharedState
            .registerObservableMeasurement[Double](makeDescriptor)
            .map(_.contramap(cast))
      }

    private def makeDescriptor: InstrumentDescriptor.Asynchronous =
      InstrumentDescriptor.asynchronous(
        name = CIString(name),
        description = description,
        unit = unit,
        instrumentType = InstrumentType.ObservableCounter
      )
  }

}
