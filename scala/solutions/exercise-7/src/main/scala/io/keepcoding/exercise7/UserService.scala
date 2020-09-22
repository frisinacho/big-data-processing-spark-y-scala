package io.keepcoding.exercise7

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait UserService { self: UserRepository =>

  /*
    1 -> Check if user exists.
         If the user does'nt exist, throw an error with the message "User not found"
    2 -> Remove the phone lines associated
    3 -> Remove the user associated
    4 -> Return the user deleted
   */

  def delete(id: String): Future[User] =
    get(id).flatMap {
      case Some(user) =>
        getPhoneLines(id).flatMap { phoneLines =>
          deletePhoneLinesInDB(phoneLines).flatMap { _ =>
            deleteInDB(user).map(_ => user)
          }
        }
      case None =>
        throw new Exception("User not found")
    }

  def deleteFun(id: String): Future[User] =
    for {
      userOpt <- get(id)
      user = userOpt.getOrElse(throw new Exception("User not found"))
      phoneLines <- getPhoneLines(id)
      _ <- deletePhoneLinesInDB(phoneLines)
      _ <- deleteInDB(user)
    } yield user
}
