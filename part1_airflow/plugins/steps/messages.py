from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='',
                        chat_id='-4252997208')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4252997208',
        'text': message
    }) # отправление сообщения

    # plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_failure_message(context):
	# ваш код здесь #
    hook = TelegramHook(telegram_conn_id='test',
                        token='7275811962:AAEn5Z--eOQ7Khc-ks_QPBb7rMnHWjDMTXk',
                        chat_id='-4252997208')
    dag = context['dag']
    run_id = context['run_id']
    ti = context['task_instance_key_str']
    
    message = f'В исполнений DAG {dag} с id={run_id} произошла ошибка! {ti}' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4252997208',
        'text': message
    }) # отправление сообщения 
