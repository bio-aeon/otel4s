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

package org.typelevel.otel4s.sdk.metrics.internal

import cats.Show
import cats.kernel.laws.discipline.HashTests
import munit.DisciplineSuite
import org.scalacheck.Prop
import org.typelevel.otel4s.sdk.metrics.scalacheck.Arbitraries._
import org.typelevel.otel4s.sdk.metrics.scalacheck.Cogens._
import org.typelevel.otel4s.sdk.metrics.scalacheck.Gens

class InstrumentDescriptorSuite extends DisciplineSuite {

  checkAll(
    "InstrumentDescriptor.HashLaws",
    HashTests[InstrumentDescriptor].hash
  )

  test("Show[InstrumentDescriptor]") {
    Prop.forAll(Gens.instrumentDescriptor) { descriptor =>
      val description = descriptor.description match {
        case Some(value) => s"description=$value, "
        case None        => ""
      }

      val unit = descriptor.unit match {
        case Some(value) => s"unit=$value, "
        case None        => ""
      }

      val expected =
        s"InstrumentDescriptor{name=${descriptor.name}, $description${unit}type=${descriptor.instrumentType}}"

      assertEquals(Show[InstrumentDescriptor].show(descriptor), expected)
    }
  }

}