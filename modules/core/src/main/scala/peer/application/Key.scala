package peer.application

trait DataStoreKey {
  val key: String
  def id: Int = key.hashCode() % 16
}

case class PersistedDataStoreValue(value: Any, time: Long = System.currentTimeMillis) {
  val creationTimestamp: Long = time
}

case class Key(key: String) extends DataStoreKey {
  def md5Hashing(str: String): Int = {
    import java.math.BigInteger
    import java.security.MessageDigest

    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(key.getBytes)
    val bigInt = new BigInteger(1, digest)

    bigInt.intValue().abs
  }

  override def hashCode(): Int = md5Hashing(key)
  override def toString: String = s"key: $key | id: $id"
}
