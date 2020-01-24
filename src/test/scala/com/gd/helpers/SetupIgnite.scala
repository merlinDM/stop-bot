package com.gd.helpers

import org.apache.ignite.configuration.IgniteConfiguration
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

    ignite = Ignition.getOrStart(igniteConfig)
  }

  override protected def afterAll(): Unit = {

    ignite.close()

  }
}
