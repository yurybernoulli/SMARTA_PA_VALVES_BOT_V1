import logging
import os
import pandas as pd
from fastapi import FastAPI, Request
import uvicorn

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import KeyboardButton, ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# === CONFIG ===
TOKEN = os.getenv("TOKEN")  # токен берем из переменных окружения
EXCEL_PATH = "База.xlsx"
OWNER_ID = 747253701

# === Загрузка Excel ===
df_raw = pd.read_excel(EXCEL_PATH)
df = df_raw.rename(columns={"Материал уплотнения ": "Материал уплотнения"})

FILTER_COLUMNS = [
    "Тип управления", "Название клапана", "Присоединение", "Dn, мм", "Обратная связь",
    "Рабочее давление среды, МПа", "Материал уплотнения",
    "Материал корпуса", "Макс. темп. рабочей среды, °С",
    "Рабочее давление управления, МПа", "Материал привода",
    "Документация", "Цена BPL",
]
RESULT_COLUMNS = ["Номер в 1С", "Маркировка", "Документация", "Цена BPL"]

class FilterState(StatesGroup):
    step = State()
    finished = State()

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO)

# === Функции клавиатуры и форматирования ===
def create_keyboard(options, step_index=0, final=False):
    kb = ReplyKeyboardBuilder()
    for option in options:
        kb.add(KeyboardButton(text=option))
    if not final:
        if step_index > 0:
            kb.add(KeyboardButton(text="⬅ Назад"))
        kb.add(KeyboardButton(text="🔁 Начать заново"))
    else:
        kb.add(KeyboardButton(text="⬅ Назад"))
        kb.add(KeyboardButton(text="🔁 Начать заново"))
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True)

def get_unique_options(column, df_subset):
    return sorted(df_subset[column].dropna().astype(str).str.strip().unique())

def format_results(dataframe, limit=30):
    lines = []
    for i, (_, row) in enumerate(dataframe.iterrows()):
        if i >= limit:
            lines.append(f"... и ещё {len(dataframe) - limit} строк")
            break
        lines.append(f"• <b>{row['Номер в 1С']}</b>\n{row['Маркировка']}\n{row['Документация']}\n{row['Цена BPL']}")
    return "\n".join(lines)

# === Хэндлеры ===
@dp.message(Command(commands=["start", "restart"]))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(FilterState.step)
    await state.update_data(step_index=0, filters={}, current_df=df.copy(), finished=False)

    column = FILTER_COLUMNS[0]
    options = get_unique_options(column, df)
    await message.answer(
        f"Выберите значение для фильтра: «<b>{column}</b>»",
        reply_markup=create_keyboard(options, step_index=0)
    )

@dp.message(Command(commands=["reset"]))
async def cmd_reset(message: types.Message, state: FSMContext):
    await cmd_start(message, state)

@dp.message(F.text == "🔁 Начать заново")
async def handle_restart_button(message: types.Message, state: FSMContext):
    await cmd_reset(message, state)

@dp.message(FilterState.step)
async def handle_step(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if data.get("finished"):
        return

    msg_text = message.text.strip()
    step_index = data.get("step_index", 0)
    filters = data.get("filters", {})
    current_df = data.get("current_df", df.copy())

    if msg_text == "🔁 Начать заново":
        await cmd_reset(message, state)
        return

    if msg_text == "⬅ Назад":
        if step_index == 0:
            await message.answer("⛔ Вы на первом шаге.")
            return
        step_index -= 1
        filters.pop(FILTER_COLUMNS[step_index], None)
        new_df = df.copy()
        for col, val in filters.items():
            new_df = new_df[new_df[col].astype(str).str.strip() == val]
        await state.update_data(step_index=step_index, filters=filters, current_df=new_df)
        prev_column = FILTER_COLUMNS[step_index]
        options = get_unique_options(prev_column, new_df)
        await message.answer(
            f"⬅ Вернулись к: «<b>{prev_column}</b>»",
            reply_markup=create_keyboard(options, step_index=step_index)
        )
        return

    current_column = FILTER_COLUMNS[step_index]
    valid_options = current_df[current_column].dropna().astype(str).str.strip().unique()

    if msg_text not in valid_options:
        await message.answer("❌ Выберите из списка.")
        return

    filters[current_column] = msg_text
    filtered_df = current_df[current_df[current_column].astype(str).str.strip() == msg_text]

    if filtered_df.empty:
        await message.answer("❌ 0 штук. /start", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    if len(filtered_df) == 1:
        result_text = format_results(filtered_df)
        await message.answer(
            f"✅ Найден 1 вариант:\n\n{result_text}",
            reply_markup=ReplyKeyboardRemove()
        )
        file_path = "filtered_results.xlsx"
        final_df = filtered_df[df_raw.columns]
        final_df.to_excel(file_path, index=False)
        await message.answer_document(types.FSInputFile(file_path))
        os.remove(file_path)

        await state.update_data(finished=True)
        await state.clear()
        return

    if len(filtered_df) <= 20:
        result_text = format_results(filtered_df)
        await message.answer(f"✅ Найдено <b>{len(filtered_df)}</b> штук:\n\n{result_text}")

    step_index += 1
    if step_index >= len(FILTER_COLUMNS):
        await message.answer(
            "🎉 Фильтрация завершена.",
            reply_markup=ReplyKeyboardRemove()
        )
        file_path = "filtered_results.xlsx"
        final_df = filtered_df[df_raw.columns]
        final_df.to_excel(file_path, index=False)
        await message.answer_document(types.FSInputFile(file_path))
        os.remove(file_path)

        await state.update_data(finished=True)
        await state.clear()
        return

    next_column = FILTER_COLUMNS[step_index]
    next_options = get_unique_options(next_column, filtered_df)
    await state.update_data(step_index=step_index, filters=filters, current_df=filtered_df)
    await message.answer(
        f"Выберите: «<b>{next_column}</b>»",
        reply_markup=create_keyboard(next_options, step_index=step_index)
    )

# === FastAPI для Render ===
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "бот работает"}

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    update = types.Update(**data)
    await dp.feed_update(bot, update)
    return {"ok": True}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
