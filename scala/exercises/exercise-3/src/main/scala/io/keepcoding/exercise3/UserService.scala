package io.keepcoding.exercise3

trait UserService {

  protected val users: Seq[User]

  protected val phoneLines: Seq[PhoneLine]

  def getAllUsers(): Seq[User] = users

  def getAllIds(): Seq[String] = ???

  def getAllIdsWithName(): Map[String, String] = ???

  def getActiveUsers(): Seq[User] = ???

  def findUserById(id: String): Option[User] = ???

  def joinUserAndPhoneLines(): Seq[PhoneLineWithUser] = ???
}
