package io.keepcoding.exercise6

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

  def delete(id: String): Future[User] = ???
}
