import com.typesafe.config.ConfigFactory

class ConfigTest {

}
object ConfigTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("simple-lib.whatever", "This value comes from a system property")

    val conf = ConfigFactory.load()
    println("The answer is: " + conf.getString("simple-app.answer"))

    // In this simple app, we're allowing SimpleLibContext() to
    // use the default config in application ; this is exactly
    // the same as passing in ConfigFactory.load() here, so we could
    // also write "new SimpleLibContext(conf)" and it would be the same.
    // (simple-lib is a library in this same examples/ directory).
    // The point is that SimpleLibContext defaults to ConfigFactory.load()
    // but also allows us to pass in our own Config.
  }
}
