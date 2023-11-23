import logging
import json
from flask import Flask, redirect, url_for, request, abort
from threading import Thread
from waitress import serve
from paste.translogger import TransLogger
import xml.etree.ElementTree as ET
import hashlib
from io import BytesIO
from io import StringIO
# version 1.1

FULL_DEBUG = False
SIGN_ENABLED = True

KEY_PARAMS_ONE = {"tags": ["account"]} #Базовый подход
KEY_PARAMS_TWO = {"tags": ["invoice_id", "phone"]} #Работа с двумя ключевыми параметрами
KEY_PARAMS_LIST = KEY_PARAMS_TWO #Указание ключевых параметров

DEFAULT_OK_TEXT = "OK"
DEFAULT_OK_CODE = 0

DEFAULT_ERR_TEXT = "Cчет не найден"
DEFAULT_ERR_CODE = 20

INVOICE_CLOSED_ERR_TEXT = "Группа сейчас заполнена, попробуйте позже"
INVOICE_CLOSED_ERR_CODE = 21

TAG_ERR_TEXT = "Have no param in request: '%s'"
TAG_ERR_CODE = 11

INVALID_SIGN_ERR_TEXT = "Invalid sign_in in request"
INVALID_SIGN_ERR_CODE = 13

DEFAULT_DESIRED_AMOUNT = None
DEFAULT_RESERVATION_MINUTES = 5
DEFAULT_LOCALE = "UTF-8"

IP_ADDR_LISTEN = "0.0.0.0"
IP_PORT_LISTEN = 8080

LOG_FILE = "bisys3_python.log"

from ckb_decorators import try_except_decorator
# def try_except_decorator (func):
#     def wrapper(*args, **kwargs):
#         try:
#             return_value = func (*args, **kwargs)
#         except Exception as e:
#             args_str = str(args)
#             kwargs_str = str(kwargs)
#             args_str = "args=%s... ; kwargs=%s..." % (args_str[:500], kwargs_str[:1000])
#
#             error = tr.TracebackException(exc_type=type(e), exc_traceback = e.__traceback__ , exc_value =e).stack[-1]
#             logging.error(u"TR_E in %s.%s:%s FOUND: %s, ARGS %s", error.filename, error.name,
#                           error.lineno, str(e), args_str)
#
#             logging.debug(u'TR_E detail "{}" in line:{} "{} | arguments:{}"'.format(e,
#                 error.lineno,
#                 error.line,
#                 args_str)
#                           )
#             return_value = 0
#         return return_value
#     return wrapper

def get_logger():

    log_format = u'%(asctime)s %(levelname)-6s %(filename)-20s .%(lineno)-4s: %(message)s'
    logger = logging.getLogger ()
    th = TimedRotatingFileHandler (LOG_FILE, when="midnight", backupCount=10)
    th.setFormatter (logging.Formatter (log_format))
    logger.addHandler (th)

    if not FULL_DEBUG:
        logger.setLevel (logging.INFO)
    else:
        logger.setLevel (logging.DEBUG)
        console = logging.StreamHandler ()
        console.setFormatter (logging.Formatter (log_format))
        logger.addHandler (console)
        logging.info (u"Debug mode enabled!")
    return logger

class Ckb_Bisys3_Gate():
    def __init__(self, sign_key, logger=None, act1_func=None, act2_func=None):
        self.web_app = Flask ("Bisys3_Gate")
        self.threadname = "Bisys3_Gate"
        self.sign_key = sign_key
        self.timeout = 7
        if not logger:
            self.logger = get_logger()
        else:
            self.logger = logger
        self.current_xml = None
        self.act1_func = act1_func
        self.act2_func = act2_func

        def run_web_app():
            if not FULL_DEBUG: #Запуск в мультиноде
                serve(TransLogger(self.web_app, logger=self.logger), host=IP_ADDR_LISTEN, port=IP_PORT_LISTEN)
            else:
                serve (TransLogger (self.web_app, logger=self.logger), host=IP_ADDR_LISTEN, port=IP_PORT_LISTEN)
                #self.web_app.run(host=IP_ADDR_LISTEN, port=IP_PORT_LISTEN, debug=True, use_reloader=False)

        self.web_app_thread = Thread(target=run_web_app, daemon=True)

        @self.web_app.route('/bisys3', methods=['POST'])
        @try_except_decorator
        def bisys3():
            #https://docs.ckassa.ru/doc/shop-api/#format-1
            bs_request = request
            xml_response_str = ""
            xml_str = bs_request.form["params"]
            if FULL_DEBUG:
                logging.info (u"Incoming BISYS 3 request, xml_str = %s",  xml_str)
            try:
                f = StringIO(xml_str)
                self.current_xml_tree = ET.parse(f)
                self.current_xml = self.current_xml_tree.getroot()

                bs_params = {
                    "key_params":   [],
                    "pay_amount":   int(get_xml_param(self.current_xml, param_name="pay_amount", node="params")),
                    "sign":         str(get_xml_param(self.current_xml, param_name="sign")),
                    "serv_code":    str(get_xml_param(self.current_xml, param_name="serv_code", node="params")),
                    "agent_date":   str(get_xml_param (self.current_xml, param_name="agent_date", node="params")),
                    "pay_id":       str(get_xml_param (self.current_xml, param_name="pay_id", node="params")),
                    "reservation_minutes": DEFAULT_RESERVATION_MINUTES,
                    "act":          {
                        "number": int(get_xml_param (self.current_xml, param_name="act", node="params")),
                        "tags": []
                    }
                }
                #Сбор ключевых параметров
                key_params = []
                account = str(get_xml_param (self.current_xml, param_name="account", node="params"))
                if not (account == "0" or account is None):
                    logging.info (u"BISYS3 found one KEY_PARAM account = '%s'", account)
                    key_params.append({"name": "account",
                                       "value": account})
                    bs_params["key_params"] = key_params
                else:
                    for param in KEY_PARAMS_LIST["tags"]: #Обход списка ключевых параметров, если аккаунт пуст
                        value = str(get_xml_param(self.current_xml, param_name=param, node="params"))
                        if value:
                            key_params.append({"name": param,
                                               "value": value.lower()})
                            bs_params["key_params"] = key_params
                        else:
                            logging.error (u"Not found BISYS 3 param %s in request %s", param, xml_str)
                            xml_response_str = self.get_answer_xml_tag_none (bs_params, param)
                            return xml_response_str.encode (DEFAULT_LOCALE)

                if self.check_sign(xml_str, bs_params["sign"]):
                    if bs_params["serv_code"] is not None:
                        logging.info (u"Request by BISYS 3, act=%s, key_params = %s, pay_amount = %s, sign_in=%s, "
                                      u"serv_code = %s, date ='%s', pay_num='%s' ",
                                       bs_params["act"]["number"],
                                       str(bs_params["key_params"]),
                                       bs_params["pay_amount"],
                                       bs_params["sign"],
                                       bs_params["serv_code"],
                                       bs_params["agent_date"],
                                       bs_params["pay_id"],
                                       )

                        if bs_params["act"]["number"] == 1:
                            xml_response_str = self.check_pay(bs_params)
                        elif bs_params["act"]["number"] == 2:
                            xml_response_str = self.do_order(bs_params)
                        elif bs_params["act"]["number"] == 3:
                            logging.error(u"Error parsing BISYS 3, is not supporting act = %s", bs_params["act"])
                        elif bs_params["act"]["number"] == 4:
                            logging.error(u"Error parsing BISYS 3, is not supporting act = %s", bs_params["act"])
                        else:
                            logging.error(u"Error parsing BISYS 3, Unknown act = %s", bs_params["act"])
                    else:
                        xml_response_str = self.get_answer_xml_tag_none(bs_params, "serv_code")
                        logging.error (u"Error parsing BISYS 3, %s", SERV_CODE_ERR_TEXT)
                else:
                    xml_response_str = self.get_answer_xml_invalid_sign (bs_params)
                    logging.error(u"Error parsing BISYS 3, %s", INVALID_SIGN_ERR_TEXT)
                    #https://docs.ckassa.ru/doc/shop-api/#error_cods

            except Exception as e:
                logging.error(u"Error parsing BISYS 3, error = %s, xml_str = %s", str(e), xml_str)
                self.current_xml = None

            if FULL_DEBUG:
                logging.info(u"Outgoing BISYS 3 response, xml_str = %s",  xml_response_str)
            return xml_response_str

        @try_except_decorator
        def get_xml_param(xml, param_name, node=None):
            for item in xml:
                if item.tag.lower() == param_name.lower():
                    return str(item.text)
                elif item.tag == node:
                    return get_xml_param(item, param_name)
            return None

    @try_except_decorator
    def bisys3_xml(func):
        def wrapper(*args, **kwargs):
            bs_params = func(*args, **kwargs)
            self = args[0]
            xml_response = self.get_full_answer_xml(signed=SIGN_ENABLED, bs_params=bs_params)
            xml_response_str = self.xml_to_string (xml_response, header=True)
            return xml_response_str.encode(DEFAULT_LOCALE)
        return wrapper

    @try_except_decorator
    def start(self):
        self.web_app_thread.start()
        logging.debug (u"%s thread  started", self.threadname)

    @try_except_decorator
    def cancel(self):
        self.web_app_thread.join(timeout=5)
        logging.debug (u"%s thread stopped", self.threadname)

    # noinspection PyArgumentList
    @bisys3_xml
    def get_answer_xml_tag_none(self, bs_params, tag_name):
        bs_params["act"]["err_code"] = TAG_ERR_CODE
        bs_params["act"]["err_text"] = TAG_ERR_TEXT % tag_name
        return bs_params

    # noinspection PyArgumentList
    @bisys3_xml
    def get_answer_xml_invalid_sign(self, bs_params):
        bs_params["act"]["err_code"] = INVALID_SIGN_ERR_CODE
        bs_params["act"]["err_text"] = INVALID_SIGN_ERR_TEXT
        return bs_params

    # noinspection PyArgumentList
    @bisys3_xml
    def check_pay(self, bs_params):
        bs_params["act"]["err_code"] = DEFAULT_OK_CODE
        bs_params["act"]["err_text"] = DEFAULT_OK_TEXT

        if self.act1_func:
            bs_params = self.act1_func(bs_params)
            if bs_params["act"]["result"] == 20:
                bs_params["act"]["err_text"] = DEFAULT_ERR_TEXT
                bs_params["act"]["err_code"] = DEFAULT_ERR_CODE
            elif bs_params["act"]["result"] == 21:
                bs_params["act"]["err_text"] = INVOICE_CLOSED_ERR_TEXT
                bs_params["act"]["err_code"] = INVOICE_CLOSED_ERR_CODE
        else:
            bs_params["act"]["desired_amount"] = DEFAULT_DESIRED_AMOUNT

        return bs_params

    # noinspection PyArgumentList
    @bisys3_xml
    def do_order(self, bs_params):
        if self.act2_func:
            bs_params = self.act2_func(bs_params)
        else:
            bs_params["act"]["err_code"] = DEFAULT_OK_CODE
            bs_params["act"]["err_text"] = DEFAULT_OK_TEXT
            logging.info(u"Registration payment Bisys 3, pay_num='%s' skipped, because have no act2_func ",
                          bs_params["pay_id"])
        return bs_params

    @try_except_decorator
    def xml_to_string(self, xml_response, header=False):
        f = BytesIO()
        tree = ET.ElementTree(xml_response)
        if header:
            tree.write(f, xml_declaration=True, encoding=DEFAULT_LOCALE, short_empty_elements=False)
        else:
            tree.write(f, xml_declaration=False, encoding=DEFAULT_LOCALE, short_empty_elements=False)
        xml_response_str = f.getvalue()
        return xml_response_str.decode(DEFAULT_LOCALE)

    def get_bs_act_param(self, bs_params, value, default_value=None):
        if bs_params and \
                ("act" in bs_params) and (value in bs_params["act"]):
            return bs_params["act"][value]
        return default_value

    @try_except_decorator
    def get_full_answer_xml(self, error=False, signed=False, bs_params=None):
        error_code = self.get_bs_act_param(bs_params, "err_code")
        if error or (error_code and error_code != DEFAULT_OK_CODE): error = True

        xml_response = ET.Element('response')
        params = ET.SubElement(xml_response, 'params')
        err_code = ET.SubElement(params, 'err_code')
        err_text = ET.SubElement(params, 'err_text')

        bs_params["act"]["sign"] = None

        if not error:
            err_code.text = str(DEFAULT_OK_CODE)
            err_text.text = DEFAULT_OK_TEXT
            desired_amount = self.get_bs_act_param(bs_params, "desired_amount", DEFAULT_DESIRED_AMOUNT)
            if desired_amount:
                desired_amount = ET.SubElement(params, 'desired_amount')
                desired_amount.text = DEFAULT_DESIRED_AMOUNT

            # Вставляем доп теги, если их передали
            if bs_params and \
                    ("act" in bs_params) and \
                    ("tags" in bs_params["act"]):
                for tag in bs_params["act"]["tags"]:
                    tag_xml = ET.SubElement (params, str(tag["tag_name"]))
                    tag_xml.text = str(tag["tag_value"])
        else:
            err_text.text = self.get_bs_act_param(bs_params, "err_text", DEFAULT_ERR_TEXT)
            err_code.text = str(self.get_bs_act_param(bs_params, "err_code", DEFAULT_ERR_CODE))

        if signed and bs_params:
            bs_params, xml_response = self.calc_sign(bs_params, xml_response)

        logging.info (u"Answer by BISYS 3, act = '%s', keyparam = '%s', pay_amount = '%s', tags = '%s', sign_in = '%s', "
                      u"serv_code = '%s', err_code = '%s', err_text = '%s', sign_out = '%s'",
                      bs_params["act"]["number"],
                      str(bs_params["key_params"]),
                      bs_params["pay_amount"],
                      str(bs_params["act"]["tags"]),
                      bs_params["sign"],
                      bs_params["serv_code"],
                      bs_params["act"]["err_code"],
                      bs_params["act"]["err_text"],
                      bs_params["act"]["sign"]
                      )
        return xml_response

    @try_except_decorator
    def calc_sign(self, bs_params, xml_response_in):
        xml_response = xml_response_in
        if bs_params and self.sign_key:
            params = xml_response.find("params", namespaces=None)
            params_str = self.xml_to_string(params, header=False)
            params_str = params_str.replace('<params>','')
            params_str = params_str.replace('</params>','')
            params_str = params_str + bs_params["sign"] + self.sign_key
            sign_str = hashlib.md5(params_str.encode(DEFAULT_LOCALE)).hexdigest()
            sign_str = sign_str.upper()
            sign = ET.SubElement(xml_response, 'sign')
            sign.text = sign_str
            bs_params["act"]["sign"] = sign_str
            if not FULL_DEBUG:
                params_str = params_str.replace(self.sign_key, "********")
            logging.debug (u"Calculating sign bisys 3 sign_str='%s', sign = '%s'",
                           params_str, sign_str)
        return bs_params, xml_response

    @try_except_decorator
    def check_sign(self, xml_str_in, sign_in):
        sign_str = None
        if SIGN_ENABLED:
            if self.sign_key:
                xml_str_in = str(xml_str_in)
                params_s_index = xml_str_in.find("<params>") + len("<params>")
                params_t_index = xml_str_in.find("</params>")
                sign_str = xml_str_in[params_s_index:params_t_index] + self.sign_key
                sign_in_check = hashlib.md5(sign_str.encode(DEFAULT_LOCALE)).hexdigest()
                sign_in_check = sign_in_check.upper()
                if sign_in_check == sign_in:
                    if not FULL_DEBUG:
                        sign_str = sign_str.replace (self.sign_key, "********")
                    logging.debug (u"Checking bisys 3 sign_in successfully, sign_str='%s', sign = '%s'",
                               sign_str, sign_in)
                    return True
            logging.error (u"Checking bisys 3 sign_in FAIL, sign_str='%s', sign = '%s'", sign_str, sign_in)
            return False
        else:
            return True



