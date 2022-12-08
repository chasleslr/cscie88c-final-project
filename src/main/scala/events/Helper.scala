package events

import io.jvm.uuid.UUID

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

trait Helper {
  def randomId: String = {
    UUID.random.toString
  }
  def currentDate: String = {
    dateToString(Calendar.getInstance.getTime)
  }
  def dateToString(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    sdf.format(date)
  }
}
