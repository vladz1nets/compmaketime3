import os
import logging
import json

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

import schedule as sched  # імпортуємо наш модуль для розрахунків
import db

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Стан розмови
(WAIT_FOR_SHOP_NAME_OR_QPART, WAIT_QPART, WAIT_DETAIL_FILES, WAIT_SHOP_NAME, WAIT_SHOP_ACTION) = range(5)

# Створюємо тимчасову директорію для файлів, якщо її немає
if not os.path.exists('temp'):
    os.makedirs('temp')

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Привіт, я твій помічник по роботі в цеху. "
        "Передай мені файли, що містять інформацію про твої вироби – я допоможу з плануванням.\n\n"
        "Якщо хочете переглянути усі результати обробки деталей певного цеху, то напишіть назву цеху або "
        "надішліть, будь ласка, новий файл **Q_part** (Excel) для нового обрахунку."
    )
    return WAIT_FOR_SHOP_NAME_OR_QPART

async def handle_qpart_or_shop_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.document:
        # Користувач надіслав файл
        return await handle_qpart(update, context)
    elif update.message.text:
        # Користувач ввів назву цеху
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
                        # Перевірити, чи файл існує
                        if os.path.exists(result_file):
                            with open(result_file, 'rb') as f:
                                await update.message.reply_document(document=f)
                            # Після надсилання можемо видалити файл, якщо це тимчасовий файл
                            # os.remove(result_file)
                        else:
                            await update.message.reply_text(f"Файл {os.path.basename(result_file)} не знайдено.")
                break
        if not shop_found:
            await update.message.reply_text(
                f"Цех '{shop_name}' не знайдено. Будь ласка, надішліть новий файл **Q_part** (Excel) для нового обрахунку."
            )
            return WAIT_FOR_SHOP_NAME_OR_QPART
        return ConversationHandler.END
    else:
        await update.message.reply_text("Будь ласка, надішліть файл **Q_part** або введіть назву цеху.")
        return WAIT_FOR_SHOP_NAME_OR_QPART

async def handle_qpart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    document = update.message.document
    if not document:
        await update.message.reply_text("Будь ласка, надішли файл Q_part як документ.")
        return WAIT_QPART
    if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
        await update.message.reply_text("Файл має бути формату Excel (.xlsx або .xls). Спробуйте ще раз.")
        return WAIT_QPART
    file = await document.get_file()
    file_path = os.path.join('temp', f"{update.message.from_user.id}_Q_part_{document.file_name}")
    await file.download_to_drive(custom_path=file_path)
    context.user_data['q_part_file'] = file_path
    await update.message.reply_text(
        "Файл Q_part отримано.\nТепер надішли файли з деталями (Excel). "
        "Надсилай їх по одному. Коли завершиш, введи команду /done."
    )
    context.user_data['detail_files'] = []
    return WAIT_DETAIL_FILES

async def handle_detail_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    document = update.message.document
    if not document:
        await update.message.reply_text("Будь ласка, надішли файл як документ.")
        return WAIT_DETAIL_FILES
    if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
        await update.message.reply_text("Файл має бути формату Excel (.xlsx або .xls).")
        return WAIT_DETAIL_FILES
    file = await document.get_file()
    file_path = os.path.join('temp', f"{update.message.from_user.id}_detail_{document.file_name}")
    await file.download_to_drive(custom_path=file_path)
    detail_files = context.user_data.get('detail_files', [])
    detail_files.append(file_path)
    context.user_data['detail_files'] = detail_files
    await update.message.reply_text(f"Файл {document.file_name} отримано. Надсилай наступний або введи /done, якщо завершив.")
    return WAIT_DETAIL_FILES

async def done_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if 'q_part_file' not in context.user_data or not context.user_data.get('detail_files'):
        await update.message.reply_text("Необхідно надіслати як Q_part, так і хоча б один файл з деталями.")
        return WAIT_DETAIL_FILES
    await update.message.reply_text("Отримано всі файли. Розраховую результати, будь ласка, зачекайте...")
    # Встановлюємо глобальні змінні для модуля schedule
    sched.q_part_file_path = context.user_data['q_part_file']
    sched.details_file_path_list = []
    for i, path in enumerate(context.user_data['detail_files'], start=1):
        sched.details_file_path_list.append((i, path))
    try:
        schedule_file = sched.FindOptimalLoadingDiagram()
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
            await update.message.reply_text("Цей цех вже існує. Використайте /mywork для перегляду або редагування.")
            return ConversationHandler.END
        else:
            await update.message.reply_text("Цех створено та результати збережено.")
    elif action == 'редагувати':
        success, msg = db.update_shop(user_id, shop_name, schedule_file)
        if not success:
            await update.message.reply_text("Цей цех не знайдено. Ви можете створити його, ввівши 'створити'.")
            return WAIT_SHOP_ACTION
        else:
            await update.message.reply_text("Результати додано до існуючого цеху.")
    else:
        await update.message.reply_text("Невідома команда. Будь ласка, введіть 'створити' або 'редагувати'.")
        return WAIT_SHOP_ACTION

    with open(schedule_file, 'rb') as f:
        await update.message.reply_document(document=f)
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
        response += f"Цех: {shop_name}\nРезультати: {', '.join([os.path.basename(f) for f in results_list])}\n\n"
    await update.message.reply_text(response)

def main():
    TOKEN = "7652010552:AAEsK-3yjz3C2Cmx4oITA7Ap4hsE3VaAbmQ"
    if not TOKEN:
        logger.error("Вкажіть TELEGRAM_BOT_TOKEN у змінних середовища.")
        return

    application = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            WAIT_FOR_SHOP_NAME_OR_QPART: [
                MessageHandler(filters.Document.ALL, handle_qpart_or_shop_name),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_qpart_or_shop_name)
            ],
            WAIT_QPART: [MessageHandler(filters.Document.ALL, handle_qpart)],
            WAIT_DETAIL_FILES: [
                MessageHandler(filters.Document.ALL, handle_detail_file),
                CommandHandler('done', done_upload),
            ],
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

