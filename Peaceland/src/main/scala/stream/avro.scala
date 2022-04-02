package stream

import org.apache.avro.Schema
import org.apache.avro.reflect.AvroSchema
import shapeless.tag.@@
import stream.drone.{Citizen, Report, ReportID}

object avro {
  type KeyRFTag
  //type KeyRecordFormat[K] = RecordFormat[K] @@ KeyRFTag

  type ValueRFTag
  //type ValueRecordFormat[V] = RecordFormat[V] @@ ValueRFTag

  //val reportIdSchema: Schema = AvroSchema[ReportID]
  //val reportSpeedSchema: Schema = AvroSchema[Report]

  //implicit val reportIdRF: KeyRecordFormat[ReportID] = RecordFormat[Report].taggedWith[KeyRFTag]
  //implicit val reportSpeedRF: ValueRecordFormat[Report] = RecordFormat[Citizen].taggedWith[ValueRFTag]

}
