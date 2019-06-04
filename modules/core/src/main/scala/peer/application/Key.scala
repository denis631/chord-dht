package peer.application

trait DataStoreKey {
  val key: String
  def id: Int = key.hashCode() % 16
}

trait PersistedDataStoreKey extends DataStoreKey {
  val creationTimestamp: Long
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

case class PersistedKey(dataKey: DataStoreKey) extends PersistedDataStoreKey {
  val key: String = dataKey.key
  val creationTimestamp: Long = System.currentTimeMillis

  override def hashCode(): Int = dataKey.hashCode()
  override def toString: String = dataKey.toString + s" | timestamp: $creationTimestamp"
}
