from webscraping import WebScraper
from etl import PDFProcessor
from teste_bd.download import ProcessandoDados
from teste_bd.scripts_sql import PostgresDataImporter

WebScraper().run()
PDFProcessor().run()
ProcessandoDados().run()
PostgresDataImporter().run()