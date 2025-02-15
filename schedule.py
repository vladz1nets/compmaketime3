import pandas as pd
import numpy as np
import time
import os
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
import colorsys
import logging

logger = logging.getLogger(__name__)

class ScheduleRec:
    def __init__(self, PartName='', opNum=0, machine='', machineInstance=0,
                 T_start=0.0, T_finish=0.0, fileNumber=0):
        self.PartName = PartName
        self.opNum = opNum
        self.machine = machine
        self.machineInstance = machineInstance
        self.T_start = T_start
        self.T_finish = T_finish
        self.fileNumber = fileNumber  # Додаємо fileNumber

class JobRec:
    def __init__(self, partID=0, batchNo=0, batchSize=0,
                 NextOp=1, readyTime=0.0):
        self.partID = partID
        self.batchNo = batchNo
        self.batchSize = batchSize
        self.NextOp = NextOp
        self.readyTime = readyTime

def ReadBatchData(q_part_file_path):
    wsQ = pd.read_excel(q_part_file_path, sheet_name='Q_part', header=0)
    PartNames = wsQ.iloc[:, 0].astype(str).str.strip().str.lower().tolist()
    BatchQty = wsQ.iloc[:, 1].fillna(1).astype(int).tolist()
    BatchVolume = wsQ.iloc[:, 2].fillna(1).astype(int).tolist()
    return PartNames, BatchQty, BatchVolume

def ReadProcessingData(details_file_path_list, PartNames, file_numbers):
    ProcessingTimeArr = {}
    MachineAssign = {}
    partOpsCount = {}
    partOpFileMapping = {}  # Новий словник для збереження номера файлу для кожної операції

    for details_file_path in details_file_path_list:
        try:
            xls = pd.ExcelFile(details_file_path)
        except Exception as e:
            logger.error(f"Не вдалося відкрити файл {details_file_path}: {e}")
            continue
        file_number = file_numbers.get(details_file_path, 0)
        for sheet_name in xls.sheet_names:
            part_name = sheet_name.strip().lower()
            if part_name not in PartNames:
                continue
            wsPart = pd.read_excel(details_file_path, sheet_name=sheet_name, header=None)
            op_index = 0  # Локальний індекс операції в межах деталі
            for op_row in range(2, wsPart.shape[0]):
                machine_name = str(wsPart.iloc[op_row, 0]).strip()
                if machine_name == '':
                    continue
                op_times = wsPart.iloc[op_row, 1:].tolist()
                for op_time in op_times:
                    if pd.isna(op_time) or op_time == '':
                        op_index += 1
                        continue
                    op_time_value = float(op_time)

                    # Додаємо дані про операцію
                    ProcessingTimeArr.setdefault(part_name, []).append(op_time_value)
                    MachineAssign.setdefault(part_name, []).append(machine_name)
                    partOpsCount[part_name] = partOpsCount.get(part_name, 0) + 1

                    # Зберігаємо номер файлу для кожної операції
                    op_total_index = partOpsCount[part_name] - 1  # Загальний індекс операції
                    key = (part_name, op_total_index)
                    partOpFileMapping[key] = file_number

                    op_index += 1  # Збільшуємо локальний індекс операції
    return ProcessingTimeArr, MachineAssign, partOpsCount, partOpFileMapping

def ReadMachineData(stanok_file_path):
    wsStanok = pd.read_excel(stanok_file_path, sheet_name='Stanok', header=0)
    machineAvailability = {}
    machineInstances = []
    for idx, row in wsStanok.iterrows():
        machine_name = str(row[0]).strip()
        if machine_name != '':
            mCount = int(row[1]) if not pd.isna(row[1]) else 1
            mCount = max(mCount, 1)
            machineAvailability[machine_name] = [0.0] * mCount
            for instIdx in range(1, mCount + 1):
                machineInstances.append(f"{machine_name}{instIdx}")
    return machineAvailability, machineInstances

def ComputeDynamicSchedule(PartNames, BatchQty, BatchVolume, ProcessingTimeArr,
                           MachineAssign, partOpsCount, machineAvailability, partOpFileMapping):
    jobs = []
    partsCount = len(PartNames)
    totalOpsCount = max(partOpsCount.values()) if partOpsCount else 0

    # Розрахунок кількості партій для кожної деталі
    numBatches = {}
    for p in range(partsCount):
        qty = BatchQty[p]
        volume = BatchVolume[p]
        num_batches = qty // volume
        if qty % volume != 0:
            num_batches += 1
        numBatches[p] = num_batches

    # Створення списку jobs
    for p in range(partsCount):
        nb = numBatches[p]
        qty = BatchQty[p]
        volume = BatchVolume[p]
        r = qty % volume
        for batch in range(1, nb + 1):
            if (batch == nb) and (r != 0):
                batch_size = r
            else:
                batch_size = volume
            jobs.append(JobRec(
                partID=p,
                batchNo=batch,
                batchSize=batch_size,
                NextOp=1,
                readyTime=0.0  # Для перших операцій readyTime = 0
            ))

    scheduleRecArray = []
    machineUsage = {machine: times[:] for machine, times in machineAvailability.items()}

    while True:
        # 1. Обчислюємо globalTime
        machine_times = [min(times) for times in machineUsage.values() if times]
        job_ready_times = [job.readyTime for job in jobs if job.NextOp <= totalOpsCount]
        if machine_times or job_ready_times:
            globalTime = min(machine_times + job_ready_times)
        else:
            break  # Немає доступних станків або job

        # 2. Знаходимо readyFilterBatch
        ready_batches = [
            job.batchNo for job in jobs
            if job.NextOp <= totalOpsCount and job.readyTime <= globalTime
        ]
        if ready_batches:
            readyFilterBatch = min(ready_batches)
            jobIsReadyFound = True
        else:
            jobIsReadyFound = False

        # 3. Визначення filterBatch
        if jobIsReadyFound:
            filterBatch = readyFilterBatch
        else:
            unfinished_batches = [job.batchNo for job in jobs if job.NextOp <= totalOpsCount]
            if not unfinished_batches:
                break  # Всі job завершені
            filterBatch = min(unfinished_batches)

        # 4. Перебір кандидатів
        jobsRemaining = 0
        bestCandidateFinish = float('inf')
        bestJobIndex = -1
        bestJob = None
        bestOpStart = None
        bestMachineName = None
        bestMachineIndex = None

        for idx, job in enumerate(jobs):
            if job.NextOp > totalOpsCount or job.batchNo != filterBatch:
                continue

            jobsRemaining += 1
            opNum = job.NextOp - 1
            partName = PartNames[job.partID]
            if partName not in MachineAssign or opNum >= len(MachineAssign[partName]):
                continue

            reqMachine = MachineAssign[partName][opNum]
            procTime_i = job.batchSize * ProcessingTimeArr[partName][opNum]

            if reqMachine == '':
                candidateStart = job.readyTime
                candidateFinish = candidateStart + procTime_i
                candidateMachineIndex = None
            else:
                availArr = machineUsage[reqMachine]
                earliestTime = min(availArr)
                candidateMachineIndex = availArr.index(earliestTime)
                candidateStart = max(job.readyTime, earliestTime)
                candidateFinish = candidateStart + procTime_i

            if candidateFinish < bestCandidateFinish:
                bestCandidateFinish = candidateFinish
                bestJobIndex = idx
                bestJob = job
                bestOpStart = candidateStart
                bestMachineName = reqMachine
                bestMachineIndex = candidateMachineIndex

        if jobsRemaining == 0 or bestJobIndex == -1:
            break

        # 5. Виконуємо операцію для вибраного job
        opNum = bestJob.NextOp - 1
        partName = PartNames[bestJob.partID]
        finishTime = bestOpStart + (bestJob.batchSize * ProcessingTimeArr[partName][opNum])

        # Отримуємо номер файлу для операції
        key = (partName, opNum)
        fileNumber = partOpFileMapping.get(key, 0)

        scheduleRecArray.append(ScheduleRec(
            PartName=f"{partName} (Партія {bestJob.batchNo})",
            opNum=bestJob.NextOp,
            machine=bestMachineName,
            machineInstance=bestMachineIndex + 1 if bestMachineIndex is not None else 0,
            T_start=bestOpStart,
            T_finish=finishTime,
            fileNumber=fileNumber  # Додаємо номер файлу
        ))

        # Оновлюємо час доступності станка
        if bestMachineName != '':
            machineUsage[bestMachineName][bestMachineIndex] = finishTime

        # Оновлюємо параметри обраного job
        bestJob.readyTime = finishTime
        bestJob.NextOp += 1

        # Видаляємо job, якщо всі операції виконані
        if bestJob.NextOp > partOpsCount.get(partName, 0):
            jobs.pop(bestJobIndex)

    # Обчислення makespan
    makespan = max((rec.T_finish for rec in scheduleRecArray), default=0.0)

    return makespan, scheduleRecArray

def DrawGanttChartTable(scheduleArray, makespan, machineInstances, output_filename='GanttChart.xlsx'):
    wb = Workbook()
    ws = wb.active
    ws.title = "GanttChart"

    # Обчислюємо максимальну тривалість (округлено до хв)
    maxMinutes = int(np.ceil(makespan))

    # Розбиваємо часову ось на інтервали по 10 хв
    totalIntervals = maxMinutes // 10
    if maxMinutes % 10 > 0:
        totalIntervals += 1

    # Створення заголовків для годин (кожні 6 інтервалів = 1 год)
    totalHours = int(np.ceil(totalIntervals / 6))
    for hr in range(totalHours):
        colStart = (hr * 6) + 2
        colEnd = colStart + 5
        ws.merge_cells(start_row=1, start_column=colStart, end_row=1, end_column=colEnd)
        cell = ws.cell(row=1, column=colStart)
        cell.value = f"{hr:02d}:00"
        cell.alignment = Alignment(horizontal='center', vertical='center')

    # Заголовки для інтервалів по 10 хв (другий рядок)
    for minVal in range(0, totalIntervals * 10, 10):
        col = (minVal // 10) + 2
        hourPart = minVal // 60
        minutePart = minVal % 60
        timeStr = f"{hourPart}:{minutePart:02d}"
        cell = ws.cell(row=2, column=col)
        cell.value = timeStr
        cell.alignment = Alignment(horizontal='center', vertical='center')

    # Запис імен машинних екземплярів у перший стовпець (починаємо з рядка 3)
    for idx, machine in enumerate(machineInstances):
        ws.cell(row=idx + 3, column=1, value=machine).alignment = Alignment(horizontal='center')

    # Малювання блоків операцій згідно з розкладом
    machineUsage = defaultdict(list)
    color_map = {}

    for rec in scheduleArray:
        if rec.machine == '':
            continue
        machineKey = f"{rec.machine}{rec.machineInstance}"
        if machineKey in machineInstances:
            machineRow = machineInstances.index(machineKey) + 3
        else:
            continue

        startMinute = rec.T_start
        finishMinute = int(np.ceil(rec.T_finish))
        startCol = int(startMinute // 10) + 2
        endCol = int((finishMinute - 1) // 10) + 2
        if startCol > endCol:
            endCol = startCol

        # Генеруємо унікальний колір для кожної деталі з урахуванням номера файлу
        part_identifier = f"{rec.PartName}_{rec.fileNumber}"
        if part_identifier not in color_map:
            hue = hash(part_identifier) % 360
            color = colorsys.hsv_to_rgb(hue / 360, 0.5, 0.8)
            rgb = tuple(int(255 * c) for c in color)
            hex_color = '{:02X}{:02X}{:02X}'.format(*rgb)
            color_map[part_identifier] = hex_color
        else:
            hex_color = color_map[part_identifier]
        fill_color = PatternFill(start_color=hex_color, end_color=hex_color, fill_type="solid")

        # Оновлюємо значення комірки, додаючи номер файлу
        cell_value = f"{rec.PartName} [Файл {rec.fileNumber}], Оп{rec.opNum}"

        ws.merge_cells(start_row=machineRow, start_column=startCol, end_row=machineRow, end_column=endCol)
        cell = ws.cell(row=machineRow, column=startCol)
        cell.value = cell_value
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.fill = fill_color

        # Додаємо дані для розрахунку простоїв
        machineUsage[machineKey].append((startMinute, finishMinute))

    # Розрахунок та запис простоїв для кожного машинного екземпляра
    for machineKey in machineUsage:
        totalWorkTime = sum(task[1] - task[0] for task in machineUsage[machineKey])
        totalIdleTime = maxMinutes - totalWorkTime
        idlePercentage = (totalIdleTime / maxMinutes) * 100 if maxMinutes > 0 else 0

        machineRow = machineInstances.index(machineKey) + 3
        idleCell = ws.cell(row=machineRow, column=(maxMinutes // 10) + 3)
        idleCell.value = f"Idle: {idlePercentage:.2f}%"
        idleCell.alignment = Alignment(horizontal='center')

    # Форматування діаграми
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                         top=Side(style='thin'), bottom=Side(style='thin'))
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row,
                            min_col=1, max_col=(maxMinutes // 10) + 3):
        for cell in row:
            cell.border = thin_border

    # Встановлення ширини стовпців
    columnWidth = 2
    for col in range(2, (maxMinutes // 10) + 3):
        col_letter = get_column_letter(col)
        ws.column_dimensions[col_letter].width = columnWidth

    # Додавання товстих вертикальних ліній кожні 6 колонок (1 година)
    for blockStartCol in range(2, (maxMinutes // 10) + 3, 6):
        col_letter = get_column_letter(blockStartCol)
        for row in range(1, ws.max_row + 1):
            cell = ws.cell(row=row, column=blockStartCol)
            cell.border = Border(left=Side(style='thick'))

    # Збереження файлу
    wb.save(output_filename)

def FindOptimalLoadingDiagram(q_part_file_path, details_file_path_list, stanok_file_path, file_numbers):
    # Зчитування даних
    PartNames, BatchQty, BatchVolume = ReadBatchData(q_part_file_path)
    ProcessingTimeArr, MachineAssign, partOpsCount, partOpFileMapping = ReadProcessingData(details_file_path_list, PartNames, file_numbers)
    machineAvailability, machineInstances = ReadMachineData(stanok_file_path)

    # Розрахунок розкладу
    makespan, scheduleRecArray = ComputeDynamicSchedule(
        PartNames, BatchQty, BatchVolume,
        ProcessingTimeArr, MachineAssign, partOpsCount, machineAvailability, partOpFileMapping
    )

    # Генерування діаграми Ганта
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    output_filename = f'GanttChart_{timestamp}.xlsx'
    DrawGanttChartTable(scheduleRecArray, makespan, machineInstances, output_filename=output_filename)
    return output_filename  # Повертаємо ім'я згенерованого файлу

