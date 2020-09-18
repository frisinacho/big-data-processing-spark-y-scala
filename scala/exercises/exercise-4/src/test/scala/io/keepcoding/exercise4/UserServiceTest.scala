package io.keepcoding.exercise4

import org.scalatest.{FlatSpec, MustMatchers}

class UserServiceTest extends FlatSpec with MustMatchers {

  import UserServiceTest._

  behavior of "exercise 4"

  val userServiceImpl = new UserService {

    protected val users: Seq[User] =
      Seq(user1, user2, user3, user4, user5, user6)

    protected val phoneLines: Seq[PhoneLine] =
      Seq(phoneLine1, phoneLine2, phoneLine3, phoneLine4, phoneLine5, phoneLine6, phoneLine7, phoneLine8)
  }

  it must "join users and phone lines" in {

    val expected = Seq(
      phoneLineWithUser1,
      phoneLineWithUser2,
      phoneLineWithUser3,
      phoneLineWithUser4,
      phoneLineWithUser5,
      phoneLineWithUser6,
      phoneLineWithUser7,
      phoneLineWithUser8
    )

    userServiceImpl.joinUserAndPhoneLines() must be(expected)

    userServiceImpl.joinUserAndPhoneLinesFun() must be(expected)
  }
}

object UserServiceTest {

  val user1 = User("1", "John", 25, true)
  val user2 = User("2", "John", 34, true)
  val user3 = User("3", "John", 18, true)
  val user4 = User("4", "John", 75, false)
  val user5 = User("5", "John", 23, true)
  val user6 = User("6", "John", 65, false)

  val phoneLine1 = PhoneLine("1", 11111111)
  val phoneLine2 = PhoneLine("2", 22222222)
  val phoneLine3 = PhoneLine("3", 33333333)
  val phoneLine4 = PhoneLine("4", 44444444)
  val phoneLine5 = PhoneLine("4", 55555555)
  val phoneLine6 = PhoneLine("5", 66666666)
  val phoneLine7 = PhoneLine("6", 77777777)
  val phoneLine8 = PhoneLine("6", 88888888)

  val phoneLineWithUser1 = PhoneLineWithUser("1", "John", 25, true, 11111111)
  val phoneLineWithUser2 = PhoneLineWithUser("2", "John", 34, true, 22222222)
  val phoneLineWithUser3 = PhoneLineWithUser("3", "John", 18, true, 33333333)
  val phoneLineWithUser4 = PhoneLineWithUser("4", "John", 75, false, 44444444)
  val phoneLineWithUser5 = PhoneLineWithUser("4", "John", 75, false, 55555555)
  val phoneLineWithUser6 = PhoneLineWithUser("5", "John", 23, true, 66666666)
  val phoneLineWithUser7 = PhoneLineWithUser("6", "John", 65, false, 77777777)
  val phoneLineWithUser8 = PhoneLineWithUser("6", "John", 65, false, 88888888)

}
