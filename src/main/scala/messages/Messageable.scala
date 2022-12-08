package messages

import io.circe.Json


trait Messageable {
  def key: String
  def eventType: String = {
    this.getClass.getSimpleName
  }
  def toJson: Json
  def timestamp: String
  def toMessage: Message = {
    Message(
      key = this.key,
      eventType = this.eventType,
      payload = this.toJson,
      recordedAt = this.timestamp
    )
  }
}
