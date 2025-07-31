package apicall;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;

public class BarchartPageSourceFetcher {

    public static void main(String[] args) {
        WebDriver driver = DriverManager.getDriver();

        try {
            driver.get("https://www.barchart.com/stocks/quotes/UNH/put-call-ratios");

            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(30));
            wait.until(ExpectedConditions.jsReturnsValue("return document.readyState === 'complete'"));

            // Print entire page source
            String pageSource = driver.getPageSource();
            System.out.println(pageSource);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DriverManager.quitDriver();
        }
    }
}
