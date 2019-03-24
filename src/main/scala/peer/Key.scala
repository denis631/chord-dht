package peer

trait DataStoreKey {
  val key: String
  def id: Int = key.hashCode() % 16
}

case class Key(key: String) extends DataStoreKey {
  override def hashCode(): Int = {
    import java.math.BigInteger
    import java.security.MessageDigest

    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(key.getBytes)
    val bigInt = new BigInteger(1, digest)

    bigInt.intValue().abs
  }

  override def toString: String = s"key: $key | id: $id"
}
