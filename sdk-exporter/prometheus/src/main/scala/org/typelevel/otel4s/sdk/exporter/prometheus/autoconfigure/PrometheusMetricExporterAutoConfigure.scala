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

package org.typelevel.otel4s.sdk.exporter.prometheus
package autoconfigure

import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Console
import cats.effect.syntax.resource._
import com.comcast.ip4s._
import fs2.compression.Compression
import fs2.io.net.Network
import org.typelevel.otel4s.sdk.autoconfigure.AutoConfigure
import org.typelevel.otel4s.sdk.autoconfigure.Config
import org.typelevel.otel4s.sdk.autoconfigure.ConfigurationError
import org.typelevel.otel4s.sdk.metrics.exporter.AggregationSelector
import org.typelevel.otel4s.sdk.metrics.exporter.MetricExporter

/** Autoconfigures Prometheus [[MetricExporter]].
  *
  * The configuration options:
  * {{{
  * | System property                                  | Environment variable                             | Description                                                                              |
  * |--------------------------------------------------|--------------------------------------------------|------------------------------------------------------------------------------------------|
  * | otel.exporter.prometheus.host                    | OTEL_EXPORTER_PROMETHEUS_HOST                    | The host that metrics are served on. Default is `localhost`.                             |
  * | otel.exporter.prometheus.port                    | OTEL_EXPORTER_PROMETHEUS_PORT                    | The port that metrics are served on. Default is `9464`.                                  |
  * | otel.exporter.prometheus.default.aggregation     | OTEL_EXPORTER_PROMETHEUS_DEFAULT_AGGREGATION     | Default aggregation as a function of instrument kind. Default is `default`.              |
  * | otel.exporter.prometheus.without.units           | OTEL_EXPORTER_PROMETHEUS_WITHOUT_UNITS           | If metrics are produced without a unit suffix. Default is `false`.                       |
  * | otel.exporter.prometheus.without.type.suffixes   | OTEL_EXPORTER_PROMETHEUS_WITHOUT_TYPE_SUFFIXES   | If metrics are produced without a type suffix. Default is `false`.                       |
  * | otel.exporter.prometheus.disable.scope.info      | OTEL_EXPORTER_PROMETHEUS_DISABLE_SCOPE_INFO      | If metrics are produced without a scope info metric or scope labels. Default is `false`. |
  * | otel.exporter.prometheus.disable.target.info     | OTEL_EXPORTER_PROMETHEUS_DISABLE_TARGET_INFO     | If metrics are produced without a target info metric. Default is `false`.                |
  * }}}
  *
  * @see
  *   [[https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/prometheus/#configuration]]
  */
private final class PrometheusMetricExporterAutoConfigure[
    F[_]: Network: Compression: Console
](implicit F: Async[F])
    extends AutoConfigure.WithHint[F, MetricExporter[F]](
      "PrometheusMetricExporter",
      PrometheusMetricExporterAutoConfigure.ConfigKeys.All
    )
    with AutoConfigure.Named[F, MetricExporter[F]] {

  import PrometheusMetricExporter.Defaults
  import PrometheusMetricExporterAutoConfigure.ConfigKeys

  def name: String = "prometheus"

  protected def fromConfig(config: Config): Resource[F, MetricExporter[F]] =
    for {
      host <- F.fromEither(config.getOrElse(ConfigKeys.Host, Defaults.Host)).toResource
      port <- F.fromEither(config.getOrElse(ConfigKeys.Port, Defaults.Port)).toResource
      defaultAggregation <- F
        .fromEither(config.getOrElse(ConfigKeys.DefaultAggregation, AggregationSelector.default))
        .toResource
      withoutUnits <- F.fromEither(config.getOrElse(ConfigKeys.WithoutUnits, Defaults.WithoutUnits)).toResource
      withoutTypeSuffixes <- F
        .fromEither(config.getOrElse(ConfigKeys.WithoutTypeSuffixes, Defaults.WithoutTypeSuffixes))
        .toResource
      disableScopeInfo <- F
        .fromEither(config.getOrElse(ConfigKeys.DisableScopeInfo, Defaults.DisableScopeInfo))
        .toResource
      disableTargetInfo <- F
        .fromEither(config.getOrElse(ConfigKeys.DisableTargetInfo, Defaults.DisableTargetInfo))
        .toResource
      exporter <- PrometheusMetricExporter
        .serverBuilder[F]
        .withHost(host)
        .withPort(port)
        .withDefaultAggregationSelector(defaultAggregation)
        .withWriterConfig(mkWriterConfig(withoutUnits, withoutTypeSuffixes, disableScopeInfo, disableTargetInfo))
        .build
    } yield exporter

  private implicit val hostReader: Config.Reader[Host] =
    Config.Reader.decodeWithHint("Host") { s =>
      Host.fromString(s).toRight(ConfigurationError("Cannot parse host"))
    }

  private implicit val portReader: Config.Reader[Port] =
    Config.Reader.decodeWithHint("Port") { s =>
      Port.fromString(s).toRight(ConfigurationError("Cannot parse port"))
    }

  private implicit val defaultAggregationReader: Config.Reader[AggregationSelector] =
    Config.Reader.decodeWithHint("Aggregation") {
      case "default" => Right(AggregationSelector.default)
      case s =>
        Left(
          ConfigurationError(
            s"Unrecognized default aggregation [$s]. Supported options [default]"
          )
        )
    }

  private def mkWriterConfig(
      withoutUnits: Boolean,
      withoutTypeSuffixes: Boolean,
      disableScopeInfo: Boolean,
      disableTargetInfo: Boolean
  ): PrometheusWriter.Config = {
    val config = PrometheusWriter.Config.default
    val configWithUnitsFlag = if (withoutUnits) config.withoutUnits else config.withUnits
    val configWithTypeSuffixesFlag =
      if (withoutTypeSuffixes) configWithUnitsFlag.withoutTypeSuffixes
      else configWithUnitsFlag.withTypeSuffixes

    val configWithScopeInfoFlag =
      if (disableScopeInfo) configWithTypeSuffixesFlag.disableScopeInfo
      else configWithTypeSuffixesFlag.enableScopeInfo

    if (disableTargetInfo) configWithScopeInfoFlag.disableTargetInfo
    else configWithScopeInfoFlag.enableTargetInfo
  }

}

object PrometheusMetricExporterAutoConfigure {

  private object ConfigKeys {
    val Host: Config.Key[Host] =
      Config.Key("otel.exporter.prometheus.host")

    val Port: Config.Key[Port] =
      Config.Key("otel.exporter.prometheus.port")

    val DefaultAggregation: Config.Key[AggregationSelector] =
      Config.Key("otel.exporter.prometheus.default.aggregation")

    val WithoutUnits: Config.Key[Boolean] =
      Config.Key("otel.exporter.prometheus.without.units")

    val WithoutTypeSuffixes: Config.Key[Boolean] =
      Config.Key("otel.exporter.prometheus.without.type.suffixes")

    val DisableScopeInfo: Config.Key[Boolean] =
      Config.Key("otel.exporter.prometheus.disable.scope.info")

    val DisableTargetInfo: Config.Key[Boolean] =
      Config.Key("otel.exporter.prometheus.disable.target.info")

    val All: Set[Config.Key[_]] =
      Set(Host, Port, DefaultAggregation, WithoutUnits, WithoutTypeSuffixes, DisableScopeInfo, DisableTargetInfo)
  }

  /** Returns [[org.typelevel.otel4s.sdk.autoconfigure.AutoConfigure.Named]] that configures Prometheus
    * [[org.typelevel.otel4s.sdk.metrics.exporter.MetricExporter]].
    *
    * The configuration options:
    * {{{
    * | System property                                  | Environment variable                             | Description                                                                              |
    * |--------------------------------------------------|--------------------------------------------------|------------------------------------------------------------------------------------------|
    * | otel.exporter.prometheus.host                    | OTEL_EXPORTER_PROMETHEUS_HOST                    | The host that metrics are served on. Default is `localhost`.                             |
    * | otel.exporter.prometheus.port                    | OTEL_EXPORTER_PROMETHEUS_PORT                    | The port that metrics are served on. Default is `9464`.                                  |
    * | otel.exporter.prometheus.default.aggregation     | OTEL_EXPORTER_PROMETHEUS_DEFAULT_AGGREGATION     | Default aggregation as a function of instrument kind. Default is `default`.              |
    * | otel.exporter.prometheus.without.units           | OTEL_EXPORTER_PROMETHEUS_WITHOUT_UNITS           | If metrics are produced without a unit suffix. Default is `false`.                       |
    * | otel.exporter.prometheus.without.type.suffixes   | OTEL_EXPORTER_PROMETHEUS_WITHOUT_TYPE_SUFFIXES   | If metrics are produced without a type suffix. Default is `false`.                       |
    * | otel.exporter.prometheus.disable.scope.info      | OTEL_EXPORTER_PROMETHEUS_DISABLE_SCOPE_INFO      | If metrics are produced without a scope info metric or scope labels. Default is `false`. |
    * | otel.exporter.prometheus.disable.target.info     | OTEL_EXPORTER_PROMETHEUS_DISABLE_TARGET_INFO     | If metrics are produced without a target info metric. Default is `false`.                |
    * }}}
    *
    * @see
    *   [[https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/prometheus/#configuration]]
    */
  def apply[
      F[_]: Async: Network: Compression: Console
  ]: AutoConfigure.Named[F, MetricExporter[F]] =
    new PrometheusMetricExporterAutoConfigure[F]

}
