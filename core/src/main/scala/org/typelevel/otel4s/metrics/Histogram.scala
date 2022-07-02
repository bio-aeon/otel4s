/*
 * Copyright 2022 Typelevel
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

package org.typelevel.otel4s
package metrics

import cats.Applicative
import cats.effect.Resource
import cats.effect.Temporal
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.concurrent.duration.TimeUnit

/** A `Histogram` instrument that records values of type `A`.
  *
  * [[Histogram]] metric data points convey a population of recorded
  * measurements in a compressed format. A histogram bundles a set of events
  * into divided populations with an overall event count and aggregate sum for
  * all events.
  *
  * @tparam F
  *   the higher-kinded type of a polymorphic effect
  * @tparam A
  *   the type of the values to record. OpenTelemetry specification expects `A`
  *   to be either [[scala.Long]] or [[scala.Double]].
  */
trait Histogram[F[_], A] {

  /** Records a value with a set of attributes.
    *
    * @param value
    *   the value to record
    * @param attributes
    *   the set of attributes to associate with the value
    */
  def record(value: A, attributes: Attribute[_]*): F[Unit]
}

object Histogram {

  private val CauseKey: AttributeKey[String] = AttributeKey.string("cause")

  def noop[F[_], A](implicit F: Applicative[F]): Histogram[F, A] =
    new Histogram[F, A] {
      def record(value: A, attributes: Attribute[_]*): F[Unit] = F.unit
    }

  implicit final class HistogramSyntax[F[_]](
      private val histogram: Histogram[F, Double]
  ) extends AnyVal {

    /** Records duration of the given effect.
      *
      * @example
      *   {{{
      * val histogram: Histogram[F] = ???
      * val attributeKey = AttributeKey.string("query_name")
      *
      * def findUser(name: String) =
      *   histogram.recordDuration(TimeUnit.MILLISECONDS, Attribute(attributeKey, "find_user")).use { _ =>
      *     db.findUser(name)
      *   }
      *   }}}
      *
      * @param timeUnit
      *   the time unit of the duration measurement
      * @param attributes
      *   the set of attributes to associate with the value
      */
    def recordDuration(
        timeUnit: TimeUnit,
        attributes: Attribute[_]*
    )(implicit F: Temporal[F]): Resource[F, Unit] =
      Resource
        .makeCase(F.monotonic) { case (start, ec) =>
          for {
            end <- F.monotonic
            _ <- histogram.record(
              (end - start).toUnit(timeUnit),
              attributes ++ causeAttributes(ec): _*
            )
          } yield ()
        }
        .void

  }

  def causeAttributes(ec: Resource.ExitCase): List[Attribute[String]] =
    ec match {
      case Resource.ExitCase.Succeeded =>
        Nil
      case Resource.ExitCase.Errored(e) =>
        List(Attribute(CauseKey, e.getClass.getName))
      case Resource.ExitCase.Canceled =>
        List(Attribute(CauseKey, "canceled"))
    }

}