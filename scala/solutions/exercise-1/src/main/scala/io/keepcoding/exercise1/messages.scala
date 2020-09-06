package io.keepcoding.exercise1

import java.net.URI
import java.util.UUID

trait Printable {
  def print(): Unit
}

trait Colorable {
  val color: String
}

trait Runnable {
  def start(): Unit
  def stop(): Unit
}

sealed trait Message {
  val id: UUID
}

case class TextMessage(id: UUID, content: String) extends Message with Printable with Colorable {
  override val color: String = "Red"
  override def print(): Unit = println(toString)
}

object TextMessage {
  def apply(content: String): TextMessage = new TextMessage(UUID.randomUUID(), content)
}

case class AttachmentMessage(id: UUID, attachmentURI: URI) extends Message with Printable {
  override def print(): Unit = println(toString)
}

object AttachmentMessage {
  def apply(attachmentURI: String): AttachmentMessage = new AttachmentMessage(UUID.randomUUID(), URI.create(attachmentURI))
}

case class SoundMessage(id: UUID, soundURI: URI) extends Message with Runnable {
  override def start(): Unit = println("Running...")
  override def stop(): Unit = println("Stopping...")
}

object SoundMessage {
  def apply(soundURI: String): SoundMessage = new SoundMessage(UUID.randomUUID(), URI.create(soundURI))
}
