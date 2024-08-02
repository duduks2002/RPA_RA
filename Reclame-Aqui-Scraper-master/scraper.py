import constants
from Reclamacao import Reclamacao
from database import update_status
from utils import csv_writer, format_url
from logger import logger, write_log_file
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

import time


def scraper(driver, nome, id_page, conn, cursor):
    cursor.execute(constants.SQL_SELECT_URL, (id_page,))
    urls = cursor.fetchall()

    cont = 1
    try:
        for url in urls:
            try:
                url = format_url(url)
                driver.get(url)
                driver.implicitly_wait(8)
                logger.info('Acessando: {}'.format(url[30:]))
                time.sleep(10)

                try:
                    driver.find_element(By.CSS_SELECTOR, "p[data-testid='disabled-complaint']")
                    logger.warning(f"Reclamação desabilitada: {url}")
                    update_status(cursor, constants.SQL_ERROR_STATUS, url, id_page)
                    write_log_file(id_page, url, 'EXCEPTION', 'Reclamação desabilitada')
                    continue # Pula para a próxima reclamação
                except NoSuchElementException:
                    pass # Se o elemento não estiver presente, prossegue com o scraping

                reclamacao = create_complaint(url, driver)
                csv_writer(reclamacao.to_dict(), nome)
                logger.info('URL {} OK'.format(cont))
                cont += 1
                update_status(
                    cursor, constants.SQL_SUCCESS_STATUS, url, id_page)
                write_log_file(id_page, url)
                time.sleep(2)
            except TimeoutException as e:
                logger.error(
                    'Não foi possível acessar a reclamação, fechando e reabrindo o navegador...\n')
                driver.quit()
                driver = webdriver.Chrome()
                driver.implicitly_wait(5)
                time.sleep(3)
                cont -= 1
                continue
            except WebDriverException as web_driver_exception:
                logger.error(web_driver_exception)
                raise

        logger.info('Coleta concluída! Nome do arquivo: {}'.format(nome))
    except Exception as e:
        logger.error(e)
    finally:
        driver.quit()
        conn.commit()


def create_complaint(url, driver):
    complaint_text = driver.find_element(
        By.CSS_SELECTOR, constants.COMPLAIN_TEXT_SELECTOR).text
    complaint_title = driver.find_element(
        By.CSS_SELECTOR, constants.COMPLAIN_TITLE_SELECTOR).text
    complaint_local = driver.find_element(
        By.CSS_SELECTOR, constants.COMPLAIN_LOCAL_SELECTOR).text
    complaint_date = driver.find_element(
        By.CSS_SELECTOR, constants.COMPLAIN_DATE_SELECTOR).text
    complaint_status = driver.find_element(
        By.CSS_SELECTOR, constants.COMPLAIN_STATUS_SELECTOR).text

    reclamacao = Reclamacao(
        url,
        complaint_text,
        complaint_title,
        complaint_local,
        complaint_date,
        complaint_status,
        find_and_assign_element(
            driver, constants.COMPLAIN_CATEGORY_1_SELECTOR),
        find_and_assign_element(
            driver, constants.COMPLAIN_CATEGORY_2_SELECTOR),
        find_and_assign_element(
            driver, constants.COMPLAIN_CATEGORY_3_SELECTOR),
        find_and_assign_element(
            driver, constants.COMPLAIN_TEXT2_SELECTOR
        )
    )

    return reclamacao


def find_and_assign_element(driver, selector):
    try:
        element = driver.find_element(By.CSS_SELECTOR, selector)
        return element.text
    except NoSuchElementException:
        return '--'
