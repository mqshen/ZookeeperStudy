import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbtassembly.AssemblyPlugin.autoImport._

object Build extends sbt.Build {

  lazy val root = Project("ZookeeperStudy", file("."))
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(releaseSettings: _*)
    .settings(SbtMultiJvm.multiJvmSettings ++ multiJvmSettings: _*)
    .settings(libraryDependencies ++= Dependencies.all)
    .settings(unmanagedSourceDirectories in Test += baseDirectory.value / "multi-jvm/scala")
    .settings(XitrumPackage.skip: _*)
    .configs(MultiJvm)

  lazy val basicSettings = Seq(
      organization := "org.goldratio",
      version := "0.1.0-SNAPSHOT",
      scalaVersion := "2.11.4",
      crossScalaVersions := Seq("2.10.4", "2.11.4"),
      scalacOptions ++= Seq("-unchecked", "-deprecation"),
      resolvers ++= Seq(
        "Local Maven Repository" at "file://"+ Path.userHome.absolutePath + "/.m2/repository",
        "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
        "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
        "spray" at "http://repo.spray.io",
        "spray nightly" at "http://nightlies.spray.io/",
        "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven")
      )

  lazy val exampleSettings = basicSettings ++ noPublishing

  lazy val releaseSettings = Seq(
      publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (version.value.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases"  at nexus + "service/local/staging/deploy/maven2")
      },
      publishMavenStyle := true,
      publishArtifact in Test := false,
      pomIncludeRepository := { (repo: MavenRepository) => false },
      pomExtra := (
        <url>https://github.com/mqshen/zookeeperStudy</url>
          <licenses>
            <license>
              <name>The Apache Software License, Version 2.0</name>
              <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
              <distribution>repo</distribution>
            </license>
          </licenses>
          <scm>
            <url>git@github.com:mqshen/zookeeperStudy.git</url>
            <connection>scm:git:git@github.com:mqshen/zookeeperStudy.git</connection>
          </scm>
          <developers>
            <developer>
              <id>mqshen</id>
              <name>miaoqi shen</name>
              <email>goldratio87@gmail.com</email>
            </developer>
          </developers>
        )
    )

  def multiJvmSettings = Seq(
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // disable parallel tests
    parallelExecution in Test := false,
    // make sure that MultiJvm tests are executed by the default test target,
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiNodeResults.events,
          testResults.summaries ++ multiNodeResults.summaries)
    })

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
    // https://github.com/sbt/sbt-pgp/issues/36
    publishTo := None
  )

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences)

  lazy val crawlSettings = Seq(
    mainClass in assembly := Some("org.goldratio.Starter")
  )

  import scalariform.formatter.preferences._
  def formattingPreferences =
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(IndentSpaces, 2)

}

object Dependencies {
  val AKKA_VERSION = "2.3.7"
  val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.4.6"

  val all = Seq( zookeeper )
}
