import os
import logging
import json
import asyncio
import uuid  # Додаємо модуль для генерації унікальних ідентифікаторів
import schedule as sched
import db

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Стан розмови
(
    WAIT_FOR_SHOP_NAME_OR_QPART,
    WAIT_DETAIL_FILES,
    WAIT_STANOK_FILE,
    WAIT_SHOP_NAME,
    WAIT_SHOP_ACTION,
) = range(5)

# Створюємо тимчасову директорію для файлів, якщо її немає
if not os.path.exists('temp'):
    os.makedirs('temp')


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Привіт! Я ваш помічник з планування виробництва.\n\n"
        "Надішліть файл **Q_part** (Excel) або введіть назву цеху, щоб отримати результати."
    )
    return WAIT_FOR_SHOP_NAME_OR_QPART

async def handle_qpart_or_shop_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.document:
        return await handle_qpart(update, context)
    elif update.message.text:
        shop_name = update.message.text.strip()
        user_id = update.message.from_user.id
        rows = db.get_user_shops(user_id)
        shop_found = False
        for row_shop_name, results_json in rows:
            if row_shop_name == shop_name:
                shop_found = True
                results_list = json.loads(results_json) if results_json else []
                if not results_list:
                    await update.message.reply_text(f"У цеху '{shop_name}' немає збережених результатів.")
                else:
                    await update.message.reply_text(f"Результати для цеху '{shop_name}':")
                    for result_file in results_list:
                        if os.path.exists(result_file):
                            with open(result_file, 'rb') as f:
                                await update.message.reply_document(document=f)
                        else:
                            await update.message.reply_text(f"Файл {os.path.basename(result_file)} не знайдено.")
                break
        if not shop_found:
            await update.message.reply_text(
                f"Цех '{shop_name}' не знайдено. Будь ласка, надішліть файл **Q_part** для нового розрахунку."
            )
            return WAIT_FOR_SHOP_NAME_OR_QPART
        return ConversationHandler.END
    else:
        await update.message.reply_text("Будь ласка, надішліть файл **Q_part** або введіть назву цеху.")
        return WAIT_FOR_SHOP_NAME_OR_QPART

async def handle_qpart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    document = update.message.document
    if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
        await update.message.reply_text("Файл має бути формату Excel (.xlsx або .xls). Спробуйте ще раз.")
        return WAIT_FOR_SHOP_NAME_OR_QPART
    file = await document.get_file()
    unique_id = uuid.uuid4().hex  # Генеруємо унікальний ідентифікатор
    file_path = os.path.join('temp', f"{update.message.from_user.id}_Q_part_{unique_id}_{document.file_name}")
    await file.download_to_drive(custom_path=file_path)
    context.user_data['q_part_file'] = file_path
    await update.message.reply_text(
        "Файл Q_part отримано.\nТепер надішліть файли з деталями (Excel). "
        "Надсилайте їх по одному. Коли завершите, введіть команду /done."
    )
    context.user_data['detail_files'] = []
    context.user_data['file_numbers'] = {}
    return WAIT_DETAIL_FILES

async def handle_detail_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    document = update.message.document
    if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
        await update.message.reply_text("Файл має бути формату Excel (.xlsx або .xls).")
        return WAIT_DETAIL_FILES
    file = await document.get_file()
    unique_id = uuid.uuid4().hex  # Генеруємо унікальний ідентифікатор
    file_path = os.path.join('temp', f"{update.message.from_user.id}_detail_{unique_id}_{document.file_name}")
    await file.download_to_drive(custom_path=file_path)
    detail_files = context.user_data.get('detail_files', [])
    detail_files.append(file_path)
    context.user_data['detail_files'] = detail_files

    file_numbers = context.user_data.get('file_numbers', {})
    file_number = len(file_numbers) + 1
    file_numbers[file_path] = file_number
    context.user_data['file_numbers'] = file_numbers

    logger.info(f"Завантажено файл: {file_path}, присвоєно номер: {file_number}")

    await update.message.reply_text(
        f"Файл {document.file_name} отримано та присвоєно номер {file_number}. "
        "Надсилайте наступний або введіть /done, якщо завершили."
    )
    return WAIT_DETAIL_FILES

async def done_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if 'q_part_file' not in context.user_data or not context.user_data.get('detail_files'):
        await update.message.reply_text("Необхідно надіслати Q_part та хоча б один файл з деталями.")
        return WAIT_DETAIL_FILES
    await update.message.reply_text("Отримано всі файли деталей. Тепер надішліть файл Stanok.")
    return WAIT_STANOK_FILE

async def handle_stanok_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    document = update.message.document
    if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
        await update.message.reply_text("Файл Stanok має бути формату Excel (.xlsx або .xls).")
        return WAIT_STANOK_FILE
    file = await document.get_file()
    unique_id = uuid.uuid4().hex  # Генеруємо унікальний ідентифікатор
    file_path = os.path.join('temp', f"{update.message.from_user.id}_Stanok_{unique_id}_{document.file_name}")
    await file.download_to_drive(custom_path=file_path)
    context.user_data['stanok_file'] = file_path
    await update.message.reply_text("Отримано файл Stanok. Розраховуємо результати, будь ласка, зачекайте...")

    # Підготовка даних для розрахунків
    q_part_file_path = context.user_data['q_part_file']
    details_file_path_list = context.user_data['detail_files']
    file_numbers = context.user_data['file_numbers']
    stanok_file_path = context.user_data['stanok_file']

    try:
        loop = asyncio.get_event_loop()
        schedule_file = await loop.run_in_executor(
            None,
            sched.FindOptimalLoadingDiagram,
            q_part_file_path,
            details_file_path_list,
            stanok_file_path,
            file_numbers
        )
    except Exception as e:
        logger.error("Помилка при розрахунках: %s", e)
        await update.message.reply_text(f"Сталася помилка під час розрахунків: {e}")
        return ConversationHandler.END
    context.user_data['schedule_file'] = schedule_file
    await update.message.reply_text("Розрахунки завершено. Вкажіть, у який цех зберегти результати:")
    return WAIT_SHOP_NAME

async def handle_shop_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    shop_name = update.message.text.strip()
    context.user_data['shop_name'] = shop_name
    await update.message.reply_text("Бажаєте створити новий цех чи відредагувати існуючий?\n"
                                    "Введіть 'створити' або 'редагувати'.")
    return WAIT_SHOP_ACTION

async def handle_shop_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    action = update.message.text.strip().lower()
    shop_name = context.user_data.get('shop_name')
    schedule_file = context.user_data.get('schedule_file')
    user_id = update.message.from_user.id

    if action == 'створити':
        success, msg = db.add_shop(user_id, shop_name, schedule_file)
        if not success:
            await update.message.reply_text("Цех вже існує. Використайте 'редагувати' для додавання результатів.")
            return WAIT_SHOP_ACTION
        else:
            await update.message.reply_text("Цех створено та результати збережено.")
    elif action == 'редагувати':
        success, msg = db.update_shop(user_id, shop_name, schedule_file)
        if not success:
            await update.message.reply_text("Цех не знайдено. Ви можете створити його, ввівши 'створити'.")
            return WAIT_SHOP_ACTION
        else:
            await update.message.reply_text("Результати додано до існуючого цеху.")
    else:
        await update.message.reply_text("Невідома команда. Будь ласка, введіть 'створити' або 'редагувати'.")
        return WAIT_SHOP_ACTION

    # Надсилаємо користувачу файл з розкладом
    if os.path.exists(schedule_file):
        with open(schedule_file, 'rb') as f:
            await update.message.reply_document(document=f)
    else:
        await update.message.reply_text("Результуючий файл не знайдено.")

    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Операцію скасовано.")
    return ConversationHandler.END

async def mywork(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    rows = db.get_user_shops(user_id)
    if not rows:
        await update.message.reply_text("У вас немає збережених цехів.")
        return
    response = "Ваші цехи:\n"
    for shop_name, results_json in rows:
        results_list = json.loads(results_json) if results_json else []
        response += f"Цех: {shop_name}\nРезультати:\n"
        for f in results_list:
            response += f"- {os.path.basename(f)}\n"
        response += "\n"
    await update.message.reply_text(response)

def main():
    TOKEN = "7652010552:AAEsK-3yjz3C2Cmx4oITA7Ap4hsE3VaAbmQ"  # Замініть на ваш фактичний токен

    application = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            WAIT_FOR_SHOP_NAME_OR_QPART: [
                MessageHandler(filters.Document.ALL, handle_qpart_or_shop_name),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_qpart_or_shop_name)
            ],
            WAIT_DETAIL_FILES: [
                MessageHandler(filters.Document.ALL, handle_detail_file),
                CommandHandler('done', done_upload),
            ],
            WAIT_STANOK_FILE: [MessageHandler(filters.Document.ALL, handle_stanok_file)],
            WAIT_SHOP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_shop_name)],
            WAIT_SHOP_ACTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_shop_action)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler('mywork', mywork))
    application.run_polling()

if __name__ == '__main__':
    main()






