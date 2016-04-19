import sbt.Keys._
import sbt._


object FlowBuild extends sbt.Build {

  bintray.BintrayKeys.bintrayVcsUrl := Some("git@github.com:nimbusgo/spark-flow.git")

  override lazy val settings = super.settings ++
    Seq(
      resolvers := Seq(
        "bintray" at "http://jcenter.bintray.com",
        "softprops-maven" at "http://dl.bintray.com/content/softprops/maven",
        "mvnrepository" at "http://mvnrepository.com/artifact/"
    ),
      licenses += ("Apache",
        url("https://github.com/nimbusgo/spark-flow/blob/master/LICENSE"))
    )

  // Force dependencies to not revert scala version
  ivyScala := ivyScala.value map {_.copy(overrideScalaVersion = true)}

  run in Compile <<= Defaults.runTask(
    fullClasspath in Compile,
    mainClass in (Compile, run), runner in (Compile, run)
  )

  parallelExecution in Test := false

  val jacksonExclusion = ExclusionRule(organization="com.fasterxml.jackson.core")
  val asmExclusion = ExclusionRule(organization="asm")
  val scalaExc = ExclusionRule(organization="org.scala-lang")
  val hadoopExc = ExclusionRule(organization="org.apache.hadoop")

  val SPARK_VERSION = "1.6.0"
  val JSON4S_VERSION = "3.3.0"

  lazy val root = Project(
    id = "root",
    base = file(".")).settings(
    name := "SparkFlow",
    organization := "SparkFlow",
    version := "0.0.1",
    scalaVersion := "2.11.8",
    libraryDependencies := Seq(
      "org.ow2.asm" % "asm" % "5.1" withSources(),
      "org.apache.spark" %% "spark-core" %
        SPARK_VERSION excludeAll(asmExclusion, scalaExc),
      "org.apache.spark" %% "spark-sql" %
        SPARK_VERSION withSources() excludeAll(asmExclusion, scalaExc),
      "org.apache.spark" %% "spark-mllib" %
        SPARK_VERSION withSources() excludeAll(asmExclusion, scalaExc),
      "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.1" % "test"
        excludeAll(asmExclusion, scalaExc, hadoopExc),
      "org.scalatest" %% "scalatest" % "2.2.1" % "test"
        withSources() excludeAll scalaExc,
      "org.scala-lang" % "scala-compiler" % "2.11.8"
    )
  ) // .aggregate(exampleSubmodule) // Forces recompilation on subproject

  // For future code split.
  /*lazy val exampleSubmodule = Project(
    id = "exampleSubmodule",
    base = file("./exampleSubmodule")).settings(
    scalaVersion := "2.11.6",
    libraryDependencies := Seq()
  ) dependsOn root*/

}
