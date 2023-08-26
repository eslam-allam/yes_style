from selenium.webdriver.chrome import webdriver, options
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.relative_locator import By 
from selenium.webdriver.support.select import Select 
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlsplit
import os
import pandas as pd
from selenium.webdriver.support.color import Color
import time
from concurrent.futures import ThreadPoolExecutor, wait, Future, CancelledError
from threading import current_thread
import logging
import gzip
import shutil
import datetime
from logging.handlers import TimedRotatingFileHandler
from tqdm import tqdm
import time
import re
from langdetect import DetectorFactory, detect
from enum import Enum
from itertools import tee, islice, chain
import typing
from selenium.webdriver.common.keys import Keys
import numpy as np
import requests
from urllib3.exceptions import ConnectionError

DetectorFactory.seed = 0

LOGGING_LEVEL = logging.DEBUG
LOGGING_FOLDER = './scraping_logs'
LOGGING_FILE = f'{LOGGING_FOLDER}/yes_style.log'

if not os.path.isdir(LOGGING_FOLDER):
    os.makedirs(LOGGING_FOLDER)

def rotator(source, dest):
    with open(source, 'rb') as f_in:
        with gzip.open(f'{dest}.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(source)

def filer(default_name):
    now = datetime.datetime.now()
    folder_name = f'{LOGGING_FOLDER}/{now.strftime("%Y")}/{now.strftime("%Y-%m")}'
    if not os.path.isdir(folder_name):
        os.makedirs(folder_name)
    base_name = os.path.basename(default_name)
    return f'{folder_name}/{base_name}'

logger = logging.getLogger(__name__)

logging_formatter = logging.Formatter(
    fmt='%(asctime)s %(threadName)s %(filename)s Line.%(lineno)d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

file_handler = TimedRotatingFileHandler(filename=LOGGING_FILE, when='midnight')
file_handler.setFormatter(logging_formatter)
file_handler.namer = filer
file_handler.rotator = rotator

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging_formatter)

logger.addHandler(file_handler)

logger.setLevel(LOGGING_LEVEL)


class ProductType:
    SINGLE = 'single'
    MULTI_SIZE = 'multi-size'
    MULTI_COLOR = 'multi-color'
    MULTI_SHADE = 'multi-shade'
    MULTI_OPTION = 'multi-option'

class MediaType(Enum):
    IMAGE = 'image'
    VIDEO = 'video'

class ElementType(Enum):
    LIST = 'list'
    INPUT = 'input'

def safe_get_element(wd: webdriver.WebDriver, by: By, value:str):
    """get element from DOM without throwing exceptions if not present

    Args:
        wd (webdriver.WebDriver): the chrome driver used for this operation
        by (By): the criteria used for fetching the element e.g. class-name, id...
        value (str): value of given criteria

    Returns:
        webElement | None: the element if found, None otherwise 
    """
    try:
        element = wd.find_element(by, value)
        return element
    except NoSuchElementException:
        return None

def confirm_language(text: str, target = 'en'):
    """confirm the language of given text matches target

    Args:
        text (str): text to be tested
        target (str, optional): target language. Defaults to 'en'.

    Returns:
        str | pd.NA: the text if language matches target, pd.NA otherwise
    """
    if pd.isna(text):
        return text
    try:
        detector = detect(text)
    except Exception:
        logger.error(f'An error has occurred while confirming language of text:\n "{text}". returning pd.NA')
        return pd.NA
    if detector != target:
        return pd.NA
    return text

def change_country_and_currency(
        wd: webdriver.WebDriver, currency = 'EUR - Euro (€)', country = 'United Kingdom', 
        settings_button_selector = 'button.md-button.hide.show-gt-sm.md-button.ng-scope.md-ink-ripple',
        country_button_selector_id = 'select_4',
        country_element_type = ElementType.LIST,
        currency_button_selector = '#select_6',
        target_currency_xpath = '//md-option[starts-with(@id, "select_option_") and div[contains(string(), "{currency}")]]',
        save_button_selector = '#regionForm > md-dialog > form > md-dialog-actions > actionrow > button.md-raised.md-primary.md-button.md-ink-ripple',
        country_list_item_selector = '//md-option[starts-with(@id, "select_option_") and div[contains(string(), "{country}")]]'):
    """change the selected country and currency on the website

    Args:
        wd (webdriver.WebDriver): the driver to be used by this operation
        currency (str, optional): the currency option to be selected. Defaults to 'EUR - Euro (€)'.
        country (str, optional): the country option to be selected. Defaults to 'United Kingdom'.

    Returns:
        bool: True if country and currency were successfully changed, False otherwise
    """
    try:
        settings_button = wait_for_presence_get(wd , By.CSS_SELECTOR, settings_button_selector, 10)
        if settings_button is None:
            logger.fatal('Could not locate settings button to change currency.')
            return False
        click_element_refresh_stale(wd, settings_button, By.CSS_SELECTOR, settings_button_selector)
        country_select_dropdown = wait_for_presence_get(wd ,By.ID, country_button_selector_id)
        if country_select_dropdown is None:
            logger.fatal('Could not locate country select list to change country.')
            return False
        click_element_refresh_stale(wd, country_select_dropdown, By.ID, country_button_selector_id)
        
        time.sleep(ACTION_DELAY_SEC)
        if country_element_type == ElementType.LIST:
            target_country = wd.find_element(By.XPATH, country_list_item_selector.format(country=country))
            click_element_refresh_stale(wd, target_country, By.XPATH, country_list_item_selector.format(country=country))
        else:
            country_select_dropdown.send_keys(Keys.CONTROL + "a")
            country_select_dropdown.send_keys(Keys.DELETE)
            country_select_dropdown.send_keys(country) 
            country_select_dropdown.send_keys(Keys.ESCAPE) 
        time.sleep(ACTION_DELAY_SEC)

        logger.debug(f'{currency_button_selector=}')
        currency_select_dropdown = wait_for_presence_get(wd ,By.CSS_SELECTOR, currency_button_selector)
        if currency_select_dropdown is None:
            logger.fatal('Could not locate currency select list to change currency.')
            return False
        click_element_refresh_stale(wd, currency_select_dropdown, By.CSS_SELECTOR, currency_button_selector)

        target_currency = wd.find_element(By.XPATH, target_currency_xpath.format(currency=currency))
        click_element_refresh_stale(wd, target_currency, By.XPATH, target_currency_xpath.format(currency=currency))
        time.sleep(ACTION_DELAY_SEC)
        
        save_button = wait_for_presence_get(wd , By.CSS_SELECTOR, save_button_selector)
        if save_button is None:
            logger.fatal('Could not locate save button to change currency.')
            return False
        click_element_refresh_stale(wd, save_button, By.CSS_SELECTOR, save_button_selector)
        time.sleep(2)
        return True
    except Exception:
        logger.fatal('An unexpected error occurred while changing currency.', exc_info=True)
        return False
    
def click_element(wd: webdriver.WebDriver, element: WebElement, sleep = True):
    try:
        wd.execute_script(JAVASCRIPT_EXECUTE_CLICK, element)
        if sleep:
            time.sleep(ACTION_DELAY_SEC)
        return True
    except Exception:
        logger.error('Failed to click element...')
        return False

def click_element_refresh_stale(wd: webdriver.WebDriver, element: WebElement, by: By, locator: str, index = None, sleep = True):
    """Click the element and keep refetching and re-clicking if stale

    Args:
        wd (webdriver.WebDriver): the driver to be used by this operation
        element (WebElement): the element to be clicked
        by (By): criteria used for refetching the element if stale e.g. class-name, id...
        locator (str): the value of the given criteria
        index (int, optional): the index specifying which element to be selected if criteria would result in multiple matches. Defaults to None.

    Returns:
        webElement: the clicked element after refresh
    """
    while True:
        try:
            wd.execute_script(JAVASCRIPT_EXECUTE_CLICK, element)
            if sleep:
                time.sleep(ACTION_DELAY_SEC)
            return element
        except Exception:
            logger.debug('Could not click element. Refreshing...')
            if index is None:
                element = wd.find_element(by, locator)
            else:
                element = wd.find_elements(by, locator)[index]

def get_attribute_retry_stale(wd: webdriver.WebDriver, element: WebElement ,attribute: str, 
                              variation_details: dict[str, object], by: By, value: str, 
                              index = None, label = 'element', max_retries = 5):
    """get the specified attribute from element and refresh the element if it's stale

    Args:
        wd (webdriver.WebDriver): the driver to be used by this operation
        element (WebElement): the element that possesses the attribute
        attribute (str): name of the attribute to be fetched e.g. textContent
        variation_details (dict[str, object]): the product variant to be updated
        by (By): the criteria used to refetch the element e.g. class-name, id...
        value (str): the value of the given criteria
        index (int, optional): index specifying which element to select if the given criteria results in multiple matches. Defaults to None.
        label (str, optional): label to be used for logging the process to identify which element is stale. Defaults to 'element'.
        max_retries (int, optional): maximum number of retries. Defaults to 5.

    Returns:
        str | None: the retrieved attribute or None if max retries reached
    """
    stale_counter = 0
    result = None

    if element is None: return None
    while stale_counter < max_retries:
        try:
            result = element.get_attribute(attribute)
            break
        except StaleElementReferenceException:
            variation = get_variation_name(variation_details)
            logger.debug(f'{label} {index + 1 if index is not None else ""} in URL: "{variation_details["product_url"]}" variation: "{variation}" is stale. Refreshing...')
            if index is None:
                searched_element = safe_get_element(wd, by, value)
                if searched_element is not None:
                    element = searched_element
                else:
                    stale_counter += 1
            else:
                elements = wd.find_elements(By.CLASS_NAME, 'athenaProductImageCarousel_image')
                if len(elements) >= index + 1:
                    element = elements[index]
                else:
                    stale_counter += 1
    return result

def get_gallery_media(wd: webdriver.WebDriver, product_details: dict[str, object], media_type = MediaType.IMAGE):
    if media_type == MediaType.IMAGE:
        gallery_button_selector = '//button[contains(@class,"productDetailPage_md-button__5P_hJ") and not(span[@class="icon icon-video"])]'
        gallery_media_selector = '.productMedia_img-content__bBpAS > img'
    elif media_type == MediaType.VIDEO:
        gallery_button_selector = '//button[contains(@class,"productDetailPage_md-button__5P_hJ") and span[@class="icon icon-video"]]'
        gallery_media_selector = '.productMedia_video-container__S0RY3 > iframe'
    else:
        logger.error(f"Invalid media type: '{media_type}'")
        return product_details

    gallery_button = wait_for_presence_get(wd, By.XPATH, gallery_button_selector, must_be_visible=True)
    if gallery_button is None:
        logger.warning(f'Unable to find gallery {media_type} for URL: "{product_details["product_url"]}"')
        return product_details
    
    if not click_element(wd, gallery_button):
        logger.error(f"Failed to open gallery for product URL: \"{product_details['product_url']}\"")
        return product_details
    gallery_images: list[WebElement] = wait_for_presence_get(wd, By.CSS_SELECTOR, gallery_media_selector, multiple=True)
    for i, image in enumerate(gallery_images):
        product_details[f'product_{media_type.value}_{i + 2}'] = image.get_attribute('src')
    close_button = wait_for_presence_get(wd, By.CSS_SELECTOR, '.productMedia_closeButton__uo6Nf', must_be_visible=True)
    if not click_element(wd, close_button):
        logger.error(f"Failed to close gallery view for URL: \"{product_details['product_url']}\"")
    return product_details

def get_cover_image(wd: webdriver.WebDriver, product_details: dict[str, object]):
    cover_image: WebElement = wait_for_presence_get(wd, By.CSS_SELECTOR, '.productDetailPage_productImageCover__chqZe > img')
    product_details['product_image_1'] = cover_image.get_attribute('src')
    return product_details

def wait_for_presence_get(wd: webdriver.WebDriver, by: By, value: str, wait_for: int = 2, must_be_visible = False, multiple = False):
    """wait for presence of element before fetching from DOM

    Args:
        wd (webdriver.WebDriver): the driver to be used by this operation
        by (By): criteria used for finding the product e.g. class-name, id...
        value (str): the value of the given criteria
        wait_for (int, optional): max wait time before timing out. Defaults to 2.
        must_be_visible (bool, optional): element must be present AND currently visible on the page. Defaults to False.

    Returns:
        WebElement | None: the fetched element or None if timed out
    """
    try:
        wait_condition = EC.presence_of_element_located((by, value))
        if must_be_visible:
            wait_condition = EC.visibility_of_element_located((by, value))
        WebDriverWait(wd, wait_for).until(wait_condition)
    except TimeoutException:
        return None
    if not multiple:
        return wd.find_element(by, value)
    return wd.find_elements(by, value)

def log_missing_element(attribute_name: str, url: str = None, option: str = None, log_level = logging.ERROR):
    message = f'Failed to fetch {attribute_name}'
    if url is not None:
        message += f' for URL: "{url}"'
    if option is not None:
        if url is not None:
            message += ','
        message += f'option: "{option}"'
    message += '.'
    logger.log(log_level, message)



def current_and_next(some_iterable) -> typing.Iterator[tuple[WebElement, WebElement]]:
    items, nexts = tee(some_iterable, 2)
    nexts = chain(islice(nexts, 1, None), [None])
    return zip(items, nexts)

# //div[@class="productDetailPage_accordionContent__tZh8X productDetailPage_detailSection__8nkkA"]//li[preceding::b[contains(text(), "Colors Available")] and following::b[contains(text(), "Size")]]
def get_bullet_details(details_container: WebElement, variation_details: dict[str, object]):
    titles: list[WebElement] = wait_for_presence_get(details_container, By.XPATH, './/b', multiple=True)
    if not titles:
        log_missing_element('Detail titles', variation_details['product_url'], variation_details.get('option', 'single'), logging.WARNING)
        return variation_details
    titles = [x.get_attribute('textContent').strip() for x in titles]
    logger.debug(f'{titles=}')
    for title, next_title in current_and_next(titles):

        if next_title is None:
            selector = f'preceding::b[text() = "{title}"]'
        else:
            selector = f'preceding::b[text() = "{title}"] and following::b[text() = "{next_title}"]'
        
        logger.debug(f'{title=} -- {next_title=}')
        list_items: list[WebElement] = wait_for_presence_get(details_container, By.XPATH, f'.//li[{selector}]', multiple=True)
        if not list_items:
            log_missing_element('Detail Bullet items', variation_details['product_url'], title, logging.WARNING)
            continue
        variation_details[title] = [x.get_attribute('textContent').strip() for x in list_items]
    return variation_details


def get_variation_misc_details(wd: webdriver.WebDriver, variation_details:dict[str, object]):
    """get a variety of miscellaneous details for a given sub variant 

    Args:
        wd (webdriver.WebDriver): the driver to be used by this operation
        variation_details (dict[str, object]): the sub variant to be updated

    Returns:
        dict[str, object]: the updated sub variant
    """
    url = variation_details['product_url']
    option = variation_details.get('option', 'single')

    details_container = get_detail_container_with_text(wd, 'Details')
    if details_container is None:
        log_missing_element('details container', url, option)
        return None
    
    information_section = get_detail_section(details_container, 'Product Information')
    if information_section is None:
        log_missing_element('information section', url, option)
        return None
    
    product_id = get_table_row_data(information_section, 'Catalog No', True)
    if product_id is None:
        log_missing_element('product id', url, option)
        return None
    variation_details['variant_SKU'] = product_id.get_attribute("textContent")

    variation_details = get_bullet_details(details_container, variation_details)

    brand_section = get_detail_section(details_container, 'About brand', True)
    if brand_section is None:
        log_missing_element('About the brand section', url, option)
        return None

    brand_description = wait_for_presence_get(brand_section, By.XPATH, './/h5[text()="About brand:"]/following-sibling::div[not(@*)]')
    if brand_description is None:
        log_missing_element('Brand description', url, option)
        return None
    brand_description = brand_description.get_attribute('textContent')
    logger.debug(f'{brand_description=}')
    variation_details['about_the_brand'] = brand_description

    product_name = wait_for_presence_get(wd, By.CSS_SELECTOR, '.productDetailPage_productUpper-heading__IKsLu > h1')
    if product_name is None:
        log_missing_element('product name', url, option)
        return None
    

    variation_details['product_name'] = product_name.text.split('-', 1)[1]
    
    product_rating = wait_for_presence_get(wd ,By.XPATH, '//div[@class="button_pill-btn__Q4iqN brandInfoColumn_pill-btn__C60e_ brandInfoColumn_digit__F4JfX" and span[@class="icon icon-star"]]')
    if product_rating is None:
        log_missing_element('product rating', url, option)
        return None
    
    variation_details['product_rating'] = float(product_rating.get_attribute('aria-label').split(' ')[0].strip().removesuffix('%')) / 20
    
    number_of_reviews = wait_for_presence_get(wd, By.XPATH, '//div[@class="productDetailPage_ratingRow__VNeGN"]//span[@class="producthighlight_reviewCount__p17d1"]')
    if number_of_reviews is None:
        log_missing_element('Number of reviews', url, option)
        return None
    
    variation_details['number_of_reviews'] = int(number_of_reviews.text.strip())

    ingredients_container = get_detail_container_with_text(wd, 'Major Ingredients')

    if ingredients_container is not None:
        logger.debug(f'Found ingredients container for URL: "{url}", option, "{option}"')
        ingredients = wait_for_presence_get(ingredients_container, By.XPATH, './/span')
        if ingredients is not None:
            ingredients = ingredients.get_attribute('textContent').strip()
            logger.debug(f'{ingredients=}')
            variation_details['product_ingredients'] = ingredients

    ships_to_bahrain = wait_for_presence_get(wd, By.XPATH, '//div[@class="productDetailPage_productInfoBox__1cs9X productDetailPage_shippingInfo__JkNSL"]/h6[text()="Shipping to Bahrain"]')
    ships_to_bahrain = ships_to_bahrain is not None

    # FIXME: changing country doesn't work
    """ settings_button_selector = 'button[value="getRegionSettings"]'
    country_button_selector_id = 'shipping-destination-setting'
    country_element_type = ElementType.INPUT
    currency_button_selector_id = '#currency-setting-select'
    target_currency_xpath = '//li[@data-value="{currency}"]'
    save_button_selector = '.MuiButton-contained'
    if not change_country_and_currency(wd, 'EUR', 'Bahrain', settings_button_selector, 
                                       country_button_selector_id,
                                       country_element_type,
                                       currency_button_selector_id,
                                       target_currency_xpath,
                                       save_button_selector):
        logger.error(f'Failed to switch country to Bahrain to check shipping status for URL: "{url}"')
        return None

    ships_to_bahrain = wait_for_presence_get(wd, By.XPATH, '//div[@class="productDetailPage_productInfoBox__1cs9X productDetailPage_shippingInfo__JkNSL"]/h6[text()="Shipping to Bahrain"]')
    ships_to_bahrain = ships_to_bahrain is not None

    if not change_country_and_currency(wd, 'EUR', 'United Kingdom', settings_button_selector, 
                                       country_button_selector_id,
                                       country_element_type,
                                       currency_button_selector_id,
                                       target_currency_xpath,
                                       save_button_selector):
        logger.error(f'Failed to switch country back to United Kingdom to check shipping status for URL: "{url}"')
        return None """

    return variation_details


def get_value_from_base_name(url:str, first_splitter = '.', first_index = 0, second_splitter = '-', second_index = 0):
    """get a value from the base name of a url after splitting twice

    Args:
        url (str): the url to get the value from
        first_splitter (str, optional): first character to split on. Defaults to '.'.
        first_index (int, optional): index of element to select after first split. Defaults to 0.
        second_splitter (str, optional): second character to split on. Defaults to '-'.
        second_index (int, optional): index of element to select after second split. Defaults to 0.

    Returns:
        _type_: _description_
    """
    base_name = os.path.basename(urlsplit(url).path)
    return base_name.split(first_splitter)[first_index].split(second_splitter)[second_index].strip()

def get_old_price(wd: webdriver.WebDriver):
    """Get price of product when you reach the page before clicking on sub variants

    Args:
        wd (webdriver.WebDriver): the driver to be used by this operation

    Returns:
        str: the price of the product
    """
    try:
        return wd.find_element(By.CLASS_NAME, 'productPrice_price')
    except NoSuchElementException:
        return wd.find_element(By.CLASS_NAME, 'productPrice_fromPrice')
    
def rgb_to_hex(rgb: list):
    """Convert rgb values to hex

    Args:
        rgb (list): list of integers representing rgb values

    Returns:
        str: the hex value prefixed with #
    """
    return '#%02x%02x%02x' % (int(rgb[0]), int(rgb[1]), int(rgb[2]))

def get_detail_container_with_text(wd: webdriver.WebDriver, text: str, contains = False, multiple = False):
    if contains:
        selector = f'contains(text(), "{text}")'
    else:
        selector = f'text()="{text}"'
    return wait_for_presence_get(wd, By.XPATH, f'//div[contains(@class, "accordion_accordion__vzKcR productDetailPage_productInfoBox__1cs9X productDetailPage_accordionContainer__YBTTO") and h4[{selector}]]', multiple=multiple)

def get_detail_section(detail_container: WebElement, text: str, contains = False, multiple = False, text_element_tag = 'h5' ):
    if contains:
        selector = f'contains(text(), "{text}")'
    else:
        selector = f'text()="{text}"'
    return wait_for_presence_get(detail_container, By.XPATH, f'.//section[{text_element_tag}[{selector}]]', multiple=multiple)

def get_table_row_data(section: WebElement, text: str, contains = False, data_index = 2, multiple = False):
    if contains:
        selector = f'contains(text(), "{text}")'
    else:
        selector = f'text()="{text}"'
    return wait_for_presence_get(section, By.XPATH, f'./table//tr[td[{selector}]]/td[{data_index}]', multiple=multiple)

def get_multi_option_details(wd: webdriver.WebDriver, product_details: dict[str, object], variation_button: WebElement) -> list[dict[str, object]]:
    """get sub variants of multi option product

    Args:
        wd (webdriver.WebDriver): the chrome webdriver to be used for this operation
        product_details (dict[str, object]): the parent product of the sub variants

    Raises:
        ValueError: if product type is not color/shade/option

    Returns:
        list[dict[str, object]]: the sub variants of the parent product
    """
    
    variations = []
    if not click_element(wd, variation_button):
        logger.error(f'Failed to click variations drop down for URL: "{product_details["product_url"]}"')
        return variations
    
    options: list[WebElement] = wait_for_presence_get(wd, By.CSS_SELECTOR, 
        'div.MuiDialogContent-root.productOptions_dialogContent__iyVXZ.mui-style-16aze95 > button.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium'
        , must_be_visible=True, multiple=True)
    
    for i, option in enumerate(options):
        option = wait_for_presence_get(wd, By.CSS_SELECTOR, 
        'div.MuiDialogContent-root.productOptions_dialogContent__iyVXZ.mui-style-16aze95 > button.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium'
        , must_be_visible=True, multiple=True)[i]

        variation_details = product_details.copy()

        option_name = wait_for_presence_get(option, By.CLASS_NAME, 'productOptions_infoCol__kazld')

        if option_name is None:
            log_missing_element('product option', variation_details['product_url'])
            continue

        variation_details['option'] = re.sub(r'\n.+', '', option_name.text.strip())

        in_stock = option.get_attribute("aria-disabled") == 'false'
        variation_details['in_stock'] = in_stock
        
        if in_stock:
            price_text = option.find_element(By.CLASS_NAME, 'productOptions_priceCol__pO6IU').text
            logger.debug(f'{price_text=}')
            variation_details['price'] = float(price_text.split(" ")[0].replace(',', '.'))
        else:
            variation_details['price'] = None

        if not click_element(wd, option):
            logger.error(f'Could not select option for URL: "{variation_details["product_url"]}", option: "{variation_details["option"]}"')
            return variations
        
        
        close_button = wait_for_presence_get(wd, By.CSS_SELECTOR, '.dialog_closeButton__NtkVk', must_be_visible=True)
        if not click_element(wd, close_button):
            logger.error(f'Could not close options menu for URL: "{variation_details["product_url"]}", option: "{variation_details["option"]}"')
            return variations
        
        
        variation_details = get_cover_image(wd, variation_details)

        if not variation_details.get('product_image_1', False):
            logger.error(f'Could not find primary image from URL: "{variation_details["product_url"]}". Size: "{variation_details["size"]}"')
            continue

        variation_details = get_variation_misc_details(wd, variation_details)
        if variation_details is None:
            continue
        variations.append(variation_details)

        if not click_element(wd, variation_button):
            logger.error(f'Failed to click variations drop down for URL: "{product_details["product_url"]}"')
            return variations
        
    return variations

def create_serialized_sku(group:pd.Series, mask):
    """Serialize a group of strings based on mask

    Args:
        group (pd.Series): the group of strings to be serialized
        mask (pd.Series[bool]): a series of bools indicating the parent row to have the #1 serial number

    Returns:
        pd.Series[tuple[str, str | pd.NA]]: a new series of tuples where each tuple contains the serialized string and the parent of this string if applicable
    """
    count = 2
    serialized_skus = []
    for idx, row in group.items():
        if mask[idx]:
            serialized_skus.append((f"{row}-1", pd.NA))
        else:
            serialized_skus.append((f"{row}-{count}", row))
            count += 1
    return pd.Series(serialized_skus, index=group.index)

def get_product_variations_from_type(wd: webdriver.WebDriver, product_details: dict[str, object]):
    """get sub variants of product based on it's type

    Args:
        wd (webdriver.WebDriver): the chrome driver used for this operation
        product_details (dict[str, object]): the parent product details
        url (str): link to parent product

    Returns:
        list[dict[str, object]]: list of sub variants of parent product
    """
    variable_button = wait_for_presence_get(wd, By.CSS_SELECTOR, 'div.buyOptions_roundCornerSelect__LTxbf[aria-hidden="false"]', must_be_visible=True)
    product_variations = []
    if variable_button is not None:
        product_details['variant_type'] = ProductType.MULTI_OPTION
        product_variations = get_multi_option_details(wd, product_details, variable_button)
    else:
        product_details['variant_type'] = ProductType.SINGLE
        in_stock = wait_for_presence_get(wd, By.XPATH, '//div[@class="buyOptions_roundCornerSelect__LTxbf buyOptions_standalone__Fk4Ka" and @aria-hidden="true"]//span[@class="buyOptions_option-additional-info__nqNXv" and contains(text(), "In-Stock")]', must_be_visible=True)
        if in_stock is None:
            product_details['in_stock'] = False
        else:
            product_details['in_stock'] = True
        product_price = wait_for_presence_get(wd, By.CLASS_NAME, 'productDetailPage_sellingPrice__s6PZu', must_be_visible=True)

        if product_price is None:
            log_missing_element('Product price', product_details['product_url'])
            return []
        logger.debug(f'{product_price.text=}')
        product_details['price'] = float(product_price.text.split(' ')[0].replace(',', '.'))

        product_details = get_variation_misc_details(wd, product_details)

        if product_details is None:
            return []
        product_variations = [product_details]
    return product_variations

def get_products_from_page(wd:webdriver.WebDriver, urls: list[str], progress_bar: tqdm):
    """get product details of every url (product)

    Args:
        wd (webdriver.WebDriver): the chrome webdriver to be used for scraping
        urls (list[str]): urls representing multiple products in a single page
        product_category (str): the category of all the products
        progress_bar (tqdm): progress bar to be used for tracking scraping progress within the category

    Returns:
        pd.DataFrame: a data-frame containing all products scraped in this page
    """
    global shutdown
    df = pd.DataFrame()
    progress_bar.total = len(urls)
    progress_bar.reset()
    progress_bar.refresh()
    for url in urls:
        if shutdown:
            logger.info(f'Received shutdown signal at product: "{url}". Returning...')
            return df
        try:
            wd.get(url)
            product_details = {}
            product_variations = []
            product_details['product_url'] = url

            categories = (x.text for x in wait_for_presence_get(wd, By.CSS_SELECTOR, 
                            f'li.icon.icon-angle-right:nth-child(n+{2}) > a, li.breadcrumb_v-stroke__A4C9P > a', must_be_visible=True, multiple=True))
            for i, crumb in enumerate(categories):
                product_details[f'product_category_{i + 1}'] = crumb

            brand_name = wait_for_presence_get(wd, By.CSS_SELECTOR, 'li.breadcrumb_v-stroke__A4C9P + li > a', must_be_visible=True)

            if brand_name is None:
                logger.error(f'Could not fetch brand name for url: "{url}". Skipping...')
                continue
            product_details['brand_name'] = brand_name.text

            primary_sku = get_value_from_base_name(url, first_index=1, second_splitter=None)
            if not primary_sku.isnumeric():
                logger.error('Could not find primary SKU for URL: "%s". Skipping...', url)
                continue
            product_details['primary_SKU'] = primary_sku

            product_details = get_gallery_media(wd, product_details, MediaType.IMAGE)
            
            product_details = get_gallery_media(wd, product_details, MediaType.VIDEO)

            related_tags: list[WebElement] = wait_for_presence_get(wd, By.CLASS_NAME, 'relatedTags', multiple=True)

            if related_tags:
                related_tags = [x.get_attribute('textContent') for x in related_tags]
                logger.debug(f'Found related tags for URL: {url}. Tags: "{related_tags}"')
                product_details['related_tags'] = related_tags

            product_variations = get_product_variations_from_type(wd, product_details)
            df = pd.concat([df, pd.DataFrame(product_variations)], ignore_index=True)
        except ConnectionError:
            pass
        except Exception:
            logger.exception(f'Unexpected error with trying to fetch data in url "{url}".', exc_info=True)
        time.sleep(ACTION_DELAY_SEC)
        progress_bar.update()

    return df

def dump_page_csv(page: pd.DataFrame, category: str, page_number: int, cleanup = True):
    page_dump_folder = PAGE_DUMP_FOLDER.format(category=category)
    if not os.path.isdir(page_dump_folder):
        os.makedirs(page_dump_folder)
    file_path = f'{page_dump_folder}/page_{page_number}.csv'
    logger.info(f'Dumping category: "{category}", page: "{page_number}" to "{file_path}"...')

    if cleanup:
        page = cleanup_dataframe(page)

    page.to_csv(file_path, index=False)

def dump_category_csv(products: pd.DataFrame, category: str, cleanup = True):
    category_dump_folder = CATEGORY_DUMP_FOLDER.format(category=category)
    if not os.path.isdir(category_dump_folder):
        os.makedirs(category_dump_folder)
    
    file_path = f'{category_dump_folder}/dump.csv'
    logger.info(f'Dumping category: "{category}" to "{file_path}"...')

    if cleanup:
        page = cleanup_dataframe(page)

    products.to_csv(file_path, index=False)

def scrape_category_url(url: str):
    """Scrapes every page of the given cult_beauty category url

    Args:
        browser_options (options.Options): options to be used by the chrome webdriver
        url (str): the url to be scraped

    Returns:
        pd.DataFrame: a data-frame containing all products scraped by the driver
    """
    global shutdown
    global browser_options

    start = time.time()
    worker = current_thread()
    current_process_id = int(worker.name.split('_')[1])

    logger.debug(f'{current_process_id=}')

    category_name = url.removeprefix('https://www.yesstyle.com/en/').split('/')[0].removeprefix('beauty-').replace('-', ' ')
    worker.name = f'WORKER#{current_process_id}_{category_name}'
    progress_bar_position = (current_process_id) * 2
    with webdriver.WebDriver(browser_options) as wd:
        wd.get(f'{url}')
        time.sleep(ACTION_DELAY_SEC)
        
        product_details = pd.DataFrame()
        if not change_country_and_currency(wd, country = 'Bahrain'):
            logger.critical('Could not change currency for category: "%s". URL "%s". Skipping category...', category_name, url)
            return product_details
        logger.info('Currency changed successfully for category: "%s"', category_name)

        last_page = wait_for_presence_get(wd, By.CSS_SELECTOR, 'a.md-button.round-btn.vary.md-button.ng-scope.md-ink-ripple[ng-click="$event.preventDefault();changePage(pagination.lastPage.urlParameters)"]')
        if last_page is not None:
            last_page = get_attribute_retry_stale(wd, last_page, 'textContent', {}, By.CSS_SELECTOR, 
                                                  'a.responsivePaginationButton.responsivePageSelector.responsivePaginationButton--last'
                                                  , label='Last Page button')
            if last_page is None:
                logger.warning('Could not find last page button for URL: "%s". Assuming 1 page...', url)
                last_page = 1
            else:
                last_page = int(last_page)
                logger.debug(f'Last found page for category: "{category_name}" is {last_page}')
        inner_bar = tqdm(total=0, colour='green', position=progress_bar_position + 1, desc='Products scanned', unit='Products', leave=True)
        for page in tqdm(range(1, last_page + 1), colour='red', position= progress_bar_position, desc='Pages scanned', unit='Pages', postfix = {'category': category_name}, leave=True):
            wd.get(f'{url}#/pn={page}')
            product_links = list(set([x.get_attribute('href') for x in wait_for_presence_get(wd, By.CSS_SELECTOR, 'div.itemContainer > a', multiple=True)]))
            logger.debug(f'Number of products on page: {len(product_links)}')
            page_products = get_products_from_page(wd, product_links, inner_bar)
            product_details = pd.concat([product_details, page_products], ignore_index=True)
            dump_page_csv(page_products, category_name, page)
            if shutdown:
                logger.info(f'Received shutdown signal on page: "{page}" of category: "{category_name}". Returning...')
                return product_details
            
            time.sleep(ACTION_DELAY_SEC)
        logger.info(f'Dumping category "{category_name}". Total scrape time: {datetime.timedelta(seconds=time.time() - start)}')
        dump_category_csv(product_details, category_name)
        return product_details

def order_serialized_columns(columns: list[str], regex = r'_(\d+)'):
    """Order subset of list where elements share the same prefix and a numeral suffix

    Args:
        columns (list[str]): The elements to be ordered
        regex (regexp, optional): pattern used to identify subsets. Defaults to r'_(\d+)'.

    Returns:
        list[str]: the ordered list of elements
    """
    ordered_columns = []
    groups = {}
    for i, column in enumerate(columns):
        index = re.search(regex, column)
        if index is None or index.group(1) is None:
            ordered_columns.append(column)
            continue
        index = int(index.group(1))
        group_name = re.sub(regex, '', column)
        if group_name not in groups:
            groups[group_name] = {'starting_index': i, 'names': [{'index':index, 'name':column}]}
        else:
            groups[group_name]['names'].append({'index':index, 'name':column})
            if i < groups[group_name]['starting_index']:
                groups[group_name]['starting_index'] = i
    for group in groups.values():
        group['names'] = sorted(group['names'], key=lambda d: d['index'], reverse=True) 

        for name in group['names']:
            ordered_columns.insert(group['starting_index'], name['name'])

    return ordered_columns

def find_with_pattern(text: str, pattern = r'range:\n+([a-z ]+)', capture_group = 1, default= pd.NA):
    """Find and return substring of text using regex pattern

    Args:
        text (str): Source text that will be searched on
        pattern (regexp, optional): Pattern used for searching. Defaults to r'range:\n+([a-z ]+)'.
        capture_group (int, optional): regex capture group to be returned when found. Defaults to 1.
        default (_type_, optional): value to be returned if no match is found. Defaults to pd.NA.

    Returns:
        str | default: Found match or default if no match found
    """
    if pd.isna(text):
        return default
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match is None:
        return default
    result = match.group(capture_group)
    if result is None:
        return default
    return result

def first_not_null(items: pd.Series):
    """Get first non-null entry in series

    Args:
        items (pd.Series): The series of items to search on

    Returns:
        Any | pd.NA: first non-null item or pd.NA if none found
    """
    return next((x for x in items if not pd.isna(x)), pd.NA)

def pattern_found(text: str, pattern = r'we regret.+(?:middle east|bahrain)', negative_return = 'no', positive_return = 'yes'):
    """Search for pattern in text and return value based on it's existence

    Args:
        text (str): Source text to search on
        pattern (regexp, optional): Pattern to use for searching. Defaults to r'we regret.+(?:middle east|bahrain)'.
        negative_return (str, optional): Value to return if pattern is not found. Defaults to 'no'.
        positive_return (str, optional): Value to return if pattern is found. Defaults to 'yes'.

    Returns:
        Any: positive_return | negative_return
    """
    if pd.isna(text):
        return negative_return
    found = re.search(pattern, text, flags=re.IGNORECASE)
    if found is None:
        return positive_return
    return negative_return

def remove_pattern(text: str, pattern = r'we regret.+(?:middle east|bahrain)'):
    """Remove substrings from text based on a regex pattern  

    Args:
        text (str): source text
        pattern (regexp, optional): Pattern to use for removing substrings. Defaults to r'we regret.+(?:middle east|bahrain)'.

    Returns:
        str | pd.NA: the modified text or NA if text is None
    """
    if pd.isna(text):
        return text
    return re.sub(pattern, '', text, flags=re.IGNORECASE)

def remove_brand_name(row: pd.Series):
    """Remove brand_name from product_name for a single row

    Args:
        row (pd.Series): The data-frame row to be modified

    Returns:
        pd.Series: The new product_name after removing brand_name prefix
    """
    if (any((pd.isna(x) for x in row.values))):
        return row['product_name']
    if not (row['product_name'].casefold().startswith(row['brand_name'].casefold())):
        return row['product_name']
    new_product_name = row['product_name'].removeprefix(row['brand_name']).strip()
    return new_product_name


def capitalize_words(text: str, full_upper_only = True):
    """Capitalizes every word in input text

    Args:
        text (str): string to be capitalized
        full_upper_only (bool, optional): only perform operation if all characters in input are uppercase. Defaults to True.

    Returns:
        str | pd.NA: the string with modifications or NA if input is None
    """
    if pd.isna(text):
        return pd.NA
    if not text.isupper() and full_upper_only:
        return text
    new_text = text.lower()
    words = new_text.split(' ')
    words = [x.capitalize() for x in words]
    return ' '.join(words)

def cleanup_dataframe(dataframe: pd.DataFrame, in_place = False):
    column_names = dataframe.columns

    df = dataframe.copy(not in_place)

    logger.info('Reordering columns...')
    df = df.reindex(order_serialized_columns(df.columns), axis=1)

    if 'variant_SKU' not in column_names:
        logger.warning("Could not find variant_SKU column during cleanup. Returning...")
        return df if not in_place else None
    
    logger.info("Removing duplicate entries...")
    df.drop_duplicates(subset='variant_SKU', inplace=True, ignore_index=True)

    if 'primary_SKU' not in column_names:
        logger.warning("Could not find primary_SKU column during cleanup. Returning...")
        return df if not in_place else None
    
    mask = df['primary_SKU'] == df['variant_SKU']
    transform = df.groupby('primary_SKU')['primary_SKU'].transform(create_serialized_sku, mask)
    logger.info("Serializing primary SKU...")
    df[['serialized_primary_SKU', 'is_variant_of']] = pd.DataFrame(transform.to_list(), columns=['serialized_primary_SKU', 'is_variant_of']
                                                                , index=transform.index)

    logger.info("Dropping empty columns...")
    df.dropna(axis=1, how='all', inplace=True)

    if 'option' in column_names:
        logger.info('Removing refill options...')
        mask = df.loc[(~pd.isna(df['option'])) & df['option'].str.contains('refill', case=False)].index
        df.drop(mask, inplace=True)

        logger.info("Removing gift vouchers...")
        mask = df.loc[(~pd.isna(df['option'])) & df['option'].str.contains('€', case=False)].index
        df.drop(mask, inplace=True)

    if 'brand_name' not in column_names:
        logger.warning("Could not find brand_name column during cleanup. Returning...")
        return df if not in_place else None
    
    logger.info('Fixing brand name capitalization...')
    df['brand_name'] = df['brand_name'].transform(capitalize_words)

    logger.info('Converting all lists to new-line delimited text...')
    df = df.applymap(lambda x: '\n'.join(x) if isinstance(x, list) else x)

    logger.info('Stripping all strings in data-frame...')
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    logger.info('Reordering columns...')
    df = df.reindex(order_serialized_columns(df.columns), axis=1)

    return df if not in_place else None


def notify_telegram(bot_token, chat_ids, message) -> bool:
    responses: list[bool] = []
    for chat_id in chat_ids:
        response:dict = requests.get(f'https://api.telegram.org/bot{bot_token}/sendMessage',
                        params={'text':message,'chat_id': chat_id}).json()
        responses.append(response.get('ok', False))
    return all([x for x in responses])

def thread_done_callback(thread: Future):
    if thread.cancelled():
        return
    error = thread.exception()
    if error is not None:
        logger.critical("An error occurred during thread execution.", exc_info=error)
        if notify_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS, f'Thread ended execution with an error: {str(error)}'):
            logger.info('Telegram notification successful.')
        else:
            logger.error("Failed to send telegram message")
    else:
        logger.info("Thread completed successfully.")


if __name__ == '__main__':
    ACTION_DELAY_SEC = 1
    JAVASCRIPT_EXECUTE_CLICK = "arguments[0].click();"
    NUM_OF_WORKERS = 1
    MAX_RETRY_VARIATION = 5

    DUMP_FOLDER = './excel_dumps'
    CATEGORY_DUMP_FOLDER = f'{DUMP_FOLDER}/{{category}}'
    PAGE_DUMP_FOLDER = f'{CATEGORY_DUMP_FOLDER}/pages'

    FINAL_DATA_FRAME_WITH_PRE_CLEANUP_PATH = f'{DUMP_FOLDER}/yes_style_pre.xlsx'
    FINAL_DATA_FRAME_WITH_POST_CLEANUP_PATH = f'{DUMP_FOLDER}/yes_style_post.xlsx'

    TELEGRAM_BOT_TOKEN = '5908631649:AAF5j_gdbtw8Zza67w3jEQc8hRkgUwfvFUY'
    TELEGRAM_CHAT_IDS = ['5128147602']
    shutdown = False

    browser_options = options.Options()
    browser_options.add_argument('-disable-notifications')
    browser_options.add_argument('-headless')
    browser_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")

    CATEGORY_LINKS = [
                    'https://www.yesstyle.com/en/beauty-skin-care/list.html/bcc.15544_bpt.46',
                    'https://www.yesstyle.com/en/beauty-makeup/list.html/bcc.15479_bpt.46',
                    'https://www.yesstyle.com/en/beauty-body-care/list.html/bcc.15572_bpt.46',
                    'https://www.yesstyle.com/en/beauty-hair-care/list.html/bcc.15586_bpt.46',
                    'https://www.yesstyle.com/en/beauty-tools-brushes/list.html/bcc.15510_bpt.46',
                    'https://www.yesstyle.com/en/beauty-sun-care/list.html/bcc.15600_bpt.46',
                    'https://www.yesstyle.com/en/beauty-beauty/list.html/bcc.15478_bpt.46'
                    ]
    logger.info('Scraping started.')
    start_time = time.time()
    try:
        df = pd.DataFrame()
        executor = ThreadPoolExecutor(NUM_OF_WORKERS)
        futures = [executor.submit(scrape_category_url, link) 
                            for link in CATEGORY_LINKS]
        for future in futures:
            future.add_done_callback(thread_done_callback)
        done, _ = wait(futures)
        for done_result in done:
            if not done_result.cancelled() and done_result.exception() is None:
                df = pd.concat([df, done_result.result()], ignore_index=True)
        
        logger.info(f'Total data-frame shape: {df.shape}')

        df = cleanup_dataframe(df)

        logger.info("Exporting excel without duplicates...")
        df.to_csv(FINAL_DATA_FRAME_WITH_POST_CLEANUP_PATH, index=False)
    except KeyboardInterrupt:
        logger.info('Program interrupted by user. Aborting all queued operations...')
        shutdown = True
        executor.shutdown(cancel_futures=True)
    except CancelledError:
        logger.info('All futures cancelled successfully.')
    finally:
        logger.info('Total execution time: %s', datetime.timedelta(seconds=time.time() - start_time))