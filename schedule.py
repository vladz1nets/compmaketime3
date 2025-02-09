import pandas as pd
import numpy as np
import colorsys
import time
import os
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Alignment, PatternFill

# Глобальні змінні (вони встановлюються із завантажених файлів)
q_part_file_path = None
details_file_path_list = []  # Список кортежів: (номер файлу, шлях до файлу)

PartNames = []          # Імена деталей із Q_part
BatchQty = []           # Кількість деталей для кожного типу
BatchVolume = []        # Об’єм партії для кожного типу

ProcessingTimeDict = defaultdict(list)  # Час обробки для кожної операції
MachineAssignDict = defaultdict(list)   # Назва машини для кожної операції
partOpsCount = defaultdict(int)         # Кількість операцій для кожної деталі

# Словник для збереження номера файлу, з якого зчитано дані
fileNumberMapping = {}

class ScheduleRec:
    def __init__(self, PartName='', OpNum=0, machine='', T_start=0.0, T_finish=0.0):
        self.PartName = PartName
        self.OpNum = OpNum
        self.machine = machine
        self.T_start = T_start
        self.T_finish = T_finish

def ReadBatchData():
    global PartNames, BatchQty, BatchVolume, q_part_file_path
    wsQ = pd.read_excel(q_part_file_path, sheet_name='Q_part', header=0)
    PartNames = wsQ.iloc[:, 0].astype(str).str.strip().tolist()
    BatchQty = wsQ.iloc[:, 1].fillna(1).astype(int).tolist()
    BatchVolume = wsQ.iloc[:, 2].fillna(1).astype(int).tolist()

def ReadProcessingData():
    global ProcessingTimeDict, MachineAssignDict, partOpsCount, fileNumberMapping, details_file_path_list
    for file_num, details_file_path in details_file_path_list:
        try:
            xls = pd.ExcelFile(details_file_path)
        except Exception as e:
            print(f"Не вдалося відкрити файл {details_file_path}: {e}")
            continue
        sheet_names = xls.sheet_names
        for sheet_name in sheet_names:
            if sheet_name not in PartNames:
                continue
            try:
                wsPart = pd.read_excel(details_file_path, sheet_name=sheet_name, header=None)
            except Exception as e:
                continue
            unique_part_name = f"{sheet_name}_File{file_num}"
            fileNumberMapping[unique_part_name] = file_num
            header_row_idx = None
            for idx in range(wsPart.shape[0]):
                row_values = wsPart.iloc[idx, :].astype(str).tolist()
                if 'Назва обладнання' in row_values:
                    header_row_idx = idx
                    break
            if header_row_idx is None:
                continue
            operation_numbers = wsPart.iloc[header_row_idx, 1:].tolist()
            operation_numbers = [str(op).strip() for op in operation_numbers if not pd.isna(op)]
            ProcessingTimeArr = []
            MachineAssign = []
            for idx in range(header_row_idx + 1, wsPart.shape[0]):
                row = wsPart.iloc[idx]
                machine_name = str(row[0]).strip()
                if pd.isna(machine_name) or machine_name == '':
                    continue
                for col_idx in range(1, len(operation_numbers) + 1):
                    op_time = row[col_idx]
                    if not pd.isna(op_time) and op_time != '':
                        try:
                            op_num = int(float(operation_numbers[col_idx - 1]))
                        except ValueError:
                            continue
                        while len(ProcessingTimeArr) < op_num:
                            ProcessingTimeArr.append(0)
                            MachineAssign.append('')
                        ProcessingTimeArr[op_num - 1] = float(op_time)
                        MachineAssign[op_num - 1] = machine_name
                        if op_num > partOpsCount[unique_part_name]:
                            partOpsCount[unique_part_name] = op_num
            if not ProcessingTimeArr:
                continue
            ProcessingTimeDict[unique_part_name] = ProcessingTimeArr
            MachineAssignDict[unique_part_name] = MachineAssign

def ComputeScheduleBatches(perm):
    numBatches = {}
    uniquePartNames = list(ProcessingTimeDict.keys())
    partNameToUniqueNames = defaultdict(list)
    for unique_part_name in uniquePartNames:
        base_part_name = unique_part_name.split('_File')[0]
        partNameToUniqueNames[base_part_name].append(unique_part_name)
    for base_part_name in PartNames:
        idx = PartNames.index(base_part_name)
        qty = BatchQty[idx]
        volume = BatchVolume[idx]
        num_batches = qty // volume
        if qty % volume != 0:
            num_batches += 1
        unique_names = partNameToUniqueNames[base_part_name]
        numBatches[base_part_name] = num_batches * len(unique_names)
    maxCycles = max(numBatches.values())
    jobParts = []
    jobBatches = []
    for cycle in range(1, maxCycles + 1):
        for idx in perm:
            base_part_name = PartNames[idx]
            unique_names = partNameToUniqueNames[base_part_name]
            for unique_part_name in unique_names:
                if cycle <= numBatches[base_part_name]:
                    jobParts.append(unique_part_name)
                    jobBatches.append(cycle)
    T = {}
    CycleFinish = [0] * (maxCycles + 1)
    scheduleRecArray = []
    for j in range(1, len(jobParts) + 1):
        unique_part_name = jobParts[j - 1]
        base_part_name = unique_part_name.split('_File')[0]
        idx = PartNames.index(base_part_name)
        qty = BatchQty[idx]
        volume = BatchVolume[idx]
        num_batches = numBatches[base_part_name]
        num_ops = partOpsCount.get(unique_part_name, 0)
        if (jobBatches[j - 1] == num_batches) and (qty % volume != 0):
            currentBatchSize = qty % volume
        else:
            currentBatchSize = volume
        if jobBatches[j - 1] == 1:
            startTime = T.get((j - 1, 1), 0)
        else:
            startTime = max(T.get((j - 1, 1), 0), CycleFinish[jobBatches[j - 1] - 1])
        scheduleRecList = []
        for op in range(1, num_ops + 1):
            procTime = currentBatchSize * ProcessingTimeDict[unique_part_name][op - 1]
            if op == 1:
                T_j_op_minus_1 = startTime
            else:
                T_j_op_minus_1 = T.get((j, op - 1), 0)
            T_j_minus_1_op = T.get((j - 1, op), 0)
            T[(j, op)] = max(T_j_minus_1_op, T_j_op_minus_1) + procTime
            machine_name = MachineAssignDict[unique_part_name][op - 1]
            file_num = fileNumberMapping.get(unique_part_name, '?')
            scheduleRecList.append(ScheduleRec(
                PartName=f"{base_part_name} (Партія {jobBatches[j - 1]}) [File {file_num}]",
                OpNum=op,
                machine=machine_name,
                T_start=max(T_j_minus_1_op, T_j_op_minus_1),
                T_finish=T[(j, op)]
            ))
        scheduleRecArray.append(scheduleRecList)
    makespan = max(T.values())
    return makespan, scheduleRecArray

def PermuteParts(arr, l, r, bestPerm, bestTime, bestScheduleArray):
    if l == r:
        currentTime, schedule = ComputeScheduleBatches(arr)
        if currentTime < bestTime[0]:
            bestTime[0] = currentTime
            bestPerm[:] = arr[:]
            bestScheduleArray.clear()
            bestScheduleArray.extend(schedule)
    else:
        for i in range(l, r + 1):
            arr[l], arr[i] = arr[i], arr[l]
            PermuteParts(arr, l + 1, r, bestPerm, bestTime, bestScheduleArray)
            arr[l], arr[i] = arr[i], arr[l]

def DrawGanttChartTable(scheduleArray, makespan, output_filename='GanttChart.xlsx'):
    # Отримання списків машин та партій
    machineSet = set()
    batchSet = set()
    for recList in scheduleArray:
        for rec in recList:
            if rec and rec.machine:
                machineSet.add(rec.machine.upper())
                batchSet.add(rec.PartName)
    machineList = sorted(machineSet)
    batchList = sorted(batchSet)
    # Генерація кольорів для партій
    batchColors = {}
    num_batches = len(batchList)
    for idx, batch in enumerate(batchList):
        hue = idx / num_batches
        r, g, b = colorsys.hsv_to_rgb(hue, 0.5, 0.9)
        color_hex = '{:02X}{:02X}{:02X}'.format(int(r * 255), int(g * 255), int(b * 255))
        batchColors[batch] = color_hex
    maxMinutes = int(np.ceil(makespan))
    wb = Workbook()
    ws = wb.active
    ws.title = "GanttChart"
    totalIntervals = maxMinutes // 10
    if maxMinutes % 10 > 0:
        totalIntervals += 1
    totalHours = int(np.ceil(totalIntervals / 6))
    for hr in range(totalHours):
        colStart = (hr * 6) + 2
        colEnd = colStart + 5
        ws.merge_cells(start_row=1, start_column=colStart, end_row=1, end_column=colEnd)
        cell = ws.cell(row=1, column=colStart)
        cell.value = f"{hr:02d}:00"
        cell.alignment = Alignment(horizontal='center', vertical='center')
    for minVal in range(0, maxMinutes, 10):
        col = (minVal // 10) + 2
        hourPart = minVal // 60
        minutePart = minVal % 60
        timeStr = f"{hourPart}:{minutePart:02d}"
        cell = ws.cell(row=2, column=col)
        cell.value = timeStr
        cell.alignment = Alignment(horizontal='center', vertical='center')
    for idx, machine in enumerate(machineList):
        ws.cell(row=idx + 3, column=1, value=machine).alignment = Alignment(horizontal='center')
    for recList in scheduleArray:
        for rec in recList:
            if not rec or not rec.machine:
                continue
            machine_upper = rec.machine.upper()
            if machine_upper in machineList:
                machineRow = machineList.index(machine_upper) + 3
            else:
                continue
            startMinute = rec.T_start
            finishMinute = int(np.ceil(rec.T_finish)) - 1
            startCol = int(startMinute // 10) + 2
            endCol = int(finishMinute // 10) + 2
            if startCol > endCol:
                endCol = startCol
            batch_color_hex = batchColors.get(rec.PartName, "FFFFFF")
            fill_color = PatternFill(start_color=batch_color_hex,
                                     end_color=batch_color_hex,
                                     fill_type="solid")
            start_coord = ws.cell(row=machineRow, column=startCol).coordinate
            end_coord = ws.cell(row=machineRow, column=endCol).coordinate
            merge_range_str = f"{start_coord}:{end_coord}"
            # Якщо комірки вже об'єднані – роз'єднуємо їх
            merged_range_str = None
            for merged_range in ws.merged_cells.ranges:
                if (merged_range.min_row <= machineRow <= merged_range.max_row) and (merged_range.min_col <= startCol <= merged_range.max_col):
                    merged_range_str = merged_range.coord
                    break
            if merged_range_str is not None:
                ws.unmerge_cells(merged_range_str)
            top_left_cell = ws.cell(row=machineRow, column=startCol)
            top_left_cell.value = f"{rec.PartName}, Оп{rec.OpNum}"
            top_left_cell.alignment = Alignment(horizontal='center', vertical='center')
            top_left_cell.fill = fill_color
            ws.merge_cells(merge_range_str)
    wb.save(output_filename)

def FindOptimalLoadingDiagram():
    # Зчитування даних із Q_part та деталей
    ReadBatchData()
    ReadProcessingData()
    partsCount = len(PartNames)
    perm_indices = list(range(partsCount))
    bestPerm = perm_indices[:]
    bestTime = [float('inf')]
    bestScheduleArray = []
    PermuteParts(perm_indices, 0, partsCount - 1, bestPerm, bestTime, bestScheduleArray)
    # Вивід результатів (можна розширити за потреби)
    print("Оптимальний порядок обробки деталей:", " -> ".join([PartNames[i] for i in bestPerm]))
    print("Загальний час виготовлення (makespan):", bestTime[0], "хвилин")
    # Генеруємо унікальне ім'я файлу з урахуванням часу
    output_filename = f'GanttChart_{int(time.time())}.xlsx'
    DrawGanttChartTable(bestScheduleArray, bestTime[0], output_filename=output_filename)
    return output_filename  # Повертаємо ім'я згенерованого файлу
