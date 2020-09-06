package io.keepcoding.exercise6

import scala.concurrent.Future

trait UserRepository {

  protected var users: Set[User]

  protected var phoneLines: Set[PhoneLine]

  protected def get(id: String): Future[Option[User]]

  protected def deleteInDB(user: User): Future[Unit]

  protected def getPhoneLines(id: String): Future[Set[PhoneLine]]

  protected def deletePhoneLinesInDB(phoneLines: Set[PhoneLine]): Future[Unit]
}
