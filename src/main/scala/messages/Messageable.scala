package messages

import io.circe.Json

// TODO: trait
trait Messageable {
  def key: String
  def eventType: String = {
    this.getClass.getSimpleName
  }
  def toJson: Json
  def timestamp: String
  def toMessage: Message = {
    // create an instance of Message from the current class
    Message(
      key = this.key,
      eventType = this.eventType,
      payload = this.toJson.noSpaces, // TODO: implcits
      recordedAt = this.timestamp
    )
  }
}
