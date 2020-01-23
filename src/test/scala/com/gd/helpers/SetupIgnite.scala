package com.gd.helpers

import java.sql.Timestamp

import javax.cache.configuration.Factory
import javax.cache.expiry.{CreatedExpiryPolicy, Duration, ExpiryPolicy}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SetupIgnite extends Suite with BeforeAndAfterAll {

  protected var ignite: Ignite = _

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.ignite").setLevel(Level.OFF)

    val is = this.getClass.getClassLoader.getResourceAsStream("ignite-config.xml")
    val igniteConfig = Ignition.loadSpringBean[IgniteConfiguration](is, "ignite-test-server")
    igniteConfig.setIgniteInstanceName("ignite-test-server")

    val cacheConfig = new CacheConfiguration[String, Timestamp]()
//    cacheConfig.setName("botListTemplate")
    cacheConfig.setName("botListTemplate*")

    val expiryPolicyFactory = CreatedExpiryPolicy.factoryOf(Duration.TEN_MINUTES)
    cacheConfig.setExpiryPolicyFactory(expiryPolicyFactory)

    igniteConfig.setCacheConfiguration(cacheConfig)

    ignite = Ignition.getOrStart(igniteConfig)
  }

  override protected def afterAll(): Unit = {

    ignite.close()

  }
}
