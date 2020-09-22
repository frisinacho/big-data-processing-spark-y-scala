package io.keepcoding.exercise7

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait InMemoryUserRepository extends UserRepository {

  protected var users: Set[User]

  protected var phoneLines: Set[PhoneLine]

  protected def get(id: String): Future[Option[User]] = Future {
    println(s"Getting user with id $id")
    users.find(_.id == id)
  }

  protected def deleteInDB(user: User): Future[Unit] = Future {
    println(s"Removing user with id ${user.id}")
    users = users - user
  }

  protected def getPhoneLines(id: String): Future[Set[PhoneLine]] = Future {
    println(s"Getting phone line with user id $id")
    phoneLines.filter(_.userId == id)
  }

  protected def deletePhoneLinesInDB(phoneLines: Set[PhoneLine]): Future[Unit] = Future {
    println(s"Removing phone lines: ${phoneLines.mkString(",")}")
    this.phoneLines = this.phoneLines -- phoneLines
  }
}
