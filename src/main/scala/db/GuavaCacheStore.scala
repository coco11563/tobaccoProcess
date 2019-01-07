package db

import java.{lang, util}
import java.util.concurrent.{Callable, ConcurrentMap}

import com.google.common.cache.{Cache, CacheStats}
import com.google.common.collect.ImmutableMap

class GuavaCacheStore[K <: Serializable,V <: Serializable, N <: Serializable] extends Serializable with Cache[K, V]{

  override def getIfPresent(key: Any): V = ???

  override def get(key: K, loader: Callable[_ <: V]): V = ???

  override def getAllPresent(keys: lang.Iterable[_]): ImmutableMap[K, V] = ???

  override def put(key: K, value: V): Unit = ???

  override def putAll(m: util.Map[_ <: K, _ <: V]): Unit = ???

  override def invalidate(key: Any): Unit = ???

  override def invalidateAll(keys: lang.Iterable[_]): Unit = ???

  override def invalidateAll(): Unit = ???

  override def size(): Long = ???

  override def stats(): CacheStats = ???

  override def asMap(): ConcurrentMap[K, V] = ???

  override def cleanUp(): Unit = ???

}
