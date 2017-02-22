import java.text.SimpleDateFormat
import java.util.Locale

object Utils {
  def ConvertDomain(domainName : String) = {
    domainName.replaceAll("screenpresso", "mywebsite")
  }

  def ParseDate(dateStr : String) = {
    val dateObj = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH) parse dateStr
    new SimpleDateFormat("YYYY-MM") format dateObj
  }

  def ParseUrl(urlStr : String) = {
    var url = urlStr
    if (urlStr.startsWith("GET"))
      url = urlStr.substring(4)
    else if (urlStr.startsWith("POST"))
      url = urlStr.substring(5)

    val index = url.indexOf('?')
    if (index > 0)
      url = url.substring(0, index)

	val index2 = url.indexOf(';')
    if (index2 > 0)
      url = url.substring(0, index2)
	  
    if (url.endsWith("1.1"))
      url.substring(0, url.length - 9)
    else
      url
  }
}
