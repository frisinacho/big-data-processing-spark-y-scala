package io.keepcoding.exercise3

trait UserService {

  protected val users: Seq[User]

  protected val phoneLines: Seq[PhoneLine]

  def getAllUsers(): Seq[User] = users

  def getAllIds() = users.map(_.id)

  def getAllIdsWithName(): Map[String, String] = users.map { case User(id, name, _, _) =>
    id -> name
  }.toMap

  def getActiveUsers() = users.filter(_.active)

  def findUserById(id: String) = users.find(_.id == id)

  def joinUserAndPhoneLines(): Seq[PhoneLineWithUser] = users.flatMap { case User(id, name, age, active) =>
    phoneLines.filter(_.userId == id)
      .map { case PhoneLine(_, phoneNumber) =>
        PhoneLineWithUser(id, name, age, active, phoneNumber)
      }
  }
}
