import constants
from Reclamacao import Reclamacao
from database import update_status, db_conn
from utils import csv_writer, format_url
from logger import logger, write_log_file
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
#from selenium.webdriver.support import expected_condition as EC
from selenium.webdriver.support.ui import WebDriverWait

import time
import multiprocessing #testando a paralelização

def scraper_worker(nome, id_page, queue):
    conn, cursor = db_conn()
    # Corrigindo a passagem do argumento
    cursor.execute(constants.SQL_SELECTOR_URL, (id_page,)) 
    urls = cursor.fetchall()
    cont = 1
    try:
        # Crie um novo driver aqui
        driver = webdriver.Chrome(ChromeDriverManager().install())  
        driver.implicitly_wait(5)
        for url in urls:
            try:
                url = format_url(url)
                driver.get(url)
                
                logger.info('Acessando: {}'.format(url[30:]))
               #wait = WebDriverWait(driver, 10)
               # wait.until(EC.presence_of_element_located(
                #    (By.CSS_SELECTOR, constants.COMPLAIN_TEXT_SELECTOR)))

                try:
                    driver.find_element(By.CSS_SELECTOR, "p[data-testid='disabled-complaint']")
                    logger.warning(f"Reclamação desabilitada: {url}")
                    update_status(cursor, constants.SQL_ERROR_STATUS, url, id_page)
                    write_log_file(id_page, url, 'EXCEPTION', 'Reclamação desabilitada')
                    continue # Pula para a próxima reclamação
                except NoSuchElementException:
                    pass # Se o elemento não estiver presente, prossegue com o scraping

                reclamacao = create_complaint(url, driver)
                queue.put(reclamacao)
                #csv_writer(reclamacao.to_dict(), nome)
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
                driver = webdriver.Chrome(ChromeDriverManager().install())
                driver.implicitly_wait(5)
                time.sleep(3)
                cont -= 1
                continue
            except WebDriverException as web_driver_exception:
                logger.error(web_driver_exception)
                raise
            finally:
                # Feche o driver sempre
                driver.quit() 
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        conn.close()


def create_processes(num_processes, nome, id_page, queue):
    processes = []
    for i in range(num_processes):
        process = multiprocessing.Process(
            target=scraper_worker, args=(nome, id_page, queue))
        processes.append(process)
        process.start()
    return processes

def scraper(nome, id_page):
    conn, cursor = db_conn()
    cursor.execute(constants.SQL_SELECTOR_URL, (id_page,))
    urls = cursor.fetchall()

    # Divide a lista de URLs em partes
    num_processes = 4  # Número de processos a serem criados
    urls_parts = [urls[i::num_processes] for i in range(num_processes)]

    # Cria a fila para comunicação entre os processos
    queue = multiprocessing.Queue()

    # Cria os processos
    processes = create_processes(num_processes, nome, id_page, queue)
    # Aguarda a conclusão dos processos
    for process in processes:
        process.join()

    # Processa os dados da fila
    cont = 1
    while not queue.empty():
        reclamacao = queue.get()
        csv_writer(reclamacao.to_dict(), nome)
        logger.info(f'URL {cont} OK')
        cont += 1
        update_status(
            cursor, constants.SQL_SUCCESS_STATUS, reclamacao.url, id_page)
        write_log_file(id_page, reclamacao.url)
    logger.info('Coleta concluída! Nome do arquivo: {}'.format(nome))

    conn.commit()
    conn.close()


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