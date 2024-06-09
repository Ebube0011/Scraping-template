from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

search_terms = ['Dentist London']

def get_driver():
    # headless browser
    options = Options()
    options.add_argument("headless")
    driver = webdriver.Edge()
    #driver = webdriver.Edge(options = options)
    return driver

def quit_driver(driver):
    driver.quit()

def search_website(driver, search_term:str):
    '''
    Search for a business using search term  
    '''
    try:
        # find the search input box
        search = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@id='searchboxinput']")))
        #     lambda x: x.find_element(By.XPATH, "//input[@id='searchboxinput']")) 
    except Exception as e:
        print('Failed to load page search box')
        print(f'Exception: {e.__class__.__name__}: {str(e)}')
        return False
    else:
        # fill the search input box
        search.clear()
        search.send_keys(search_term)
        print('filled search box')
        
        # press keyboard enter
        search.send_keys(Keys.RETURN)
        #search.submit()
        driver.implicitly_wait(3)
        return True
    
def get_listings(driver):
    try:
        listings = WebDriverWait(driver, 10).until(
                    # EC.presence_of_element_located((By.CSS_SELECTOR,"a.hfpxzc")))
                    lambda x: x.find_elements(By.CSS_SELECTOR,"a.hfpxzc")) 
    except Exception as e:
        print('Failed to load search result listings in time')
        print(f'Exception: {e.__class__.__name__}: {str(e)}')
        return [None]
    else:
        if (len(listings) <= 0):
            print('No search result found found')
        else:
            # hover and scroll down
            #for result in more_results():
            # click on each individual listing
            for listing in listings:
                # click the listing to render the js
                listing.click()
                yield listing

    
    
def parse_listing(driver):

    print('parsing listings')
    name_xpath = "//h1[@class='DUwDvf lfPIob']"
    address_xpath = "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]"
    website_xpath = "//a[@data-item-id='authority']//div[contains(@class, 'fontBodyMedium')]"
    phone_no_xpath = "//button[contains(@data-item-id, 'phone:tel:')]//div[contains(@class, 'fontBodyMedium')]"
    #reviews_xpath = "//span[@role='img']"
    
    # Check if the information has loaded
    try:
        item = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, name_xpath)))
            #lambda x: x.find_element(By.XPATH, name_xpath)) 
    except:
        print('Failed to load business data in time')
        return None
    # parse the data and return the dictionary
    else:
        business = {}
        try:
            business['name'] = driver.find_element(By.XPATH, name_xpath).text
        except:
            business['name'] = ''
        try:
            business['address'] = driver.find_element(By.XPATH, address_xpath).text
        except:
            business['address'] = ''
        try:
            business['website'] = driver.find_element(By.XPATH, website_xpath).text
        except:
            business['website'] = ''
        try:
            business['phone_number'] = driver.find_element(By.XPATH, phone_no_xpath).text
        except:
            business['phone_number'] = ''
        
        #business.reviews_average = float(driver.find_elements(By.XPATH, reviews_xpath).get_attribute('aria-label').split()[0].replace(',', '-').strip())
        #business.reviews_count = int(driver.find_elements(By.XPATH, reviews_xpath).get_attribute('aria-label').split()[2].strip())
        return business


def main():

    # get driver
    driver = get_driver()

    # fetch website
    driver.get('https://www.google.com/maps')

    for search_term in search_terms:
        # search the website
        search_is_done = search_website(driver, search_term)
        if search_is_done:
            
            # define the business list object
            business_list = []
            # find the listings
            for listing in get_listings(driver):
                if (listing != None):
                    info = parse_listing(driver)
                    if (info != None):
                        # add to business_list
                        business_list.append(info)
                        driver.implicitly_wait(1)

            # save data to excel
            print(business_list)
            #business_list.save_to_excel('Output/google_maps_data')

    # close the driver
    quit_driver(driver)


if __name__ == '__main__':
    main()