// The Typesafe repository
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

// // Use the Play sbt plugin for Play projects
// addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.3.10")

// http://www.scalastyle.org/sbt.html
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
