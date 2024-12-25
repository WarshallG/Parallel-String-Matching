from kmp import kmp_search
from parallel_matching import match_pattern
import time
import os
import concurrent.futures
from tqdm import tqdm
from kmp import par_kmp_search

def match_viruses_in_files(files, pattern, idx, match_type):
    if match_type == 'match_pattern':  # 书上的并行字符串匹配算法
        func = match_pattern
    elif match_type == 'kmp_search':   # 串行KMP算法
        func = kmp_search
    elif match_type == 'par_kmp_search':  # 并行KMP算法
        func = par_kmp_search
    else:
        print("Invalid match type")
        return []
    
    results = []
    for file_path in tqdm(files, desc=f"扫描virus{idx+1}", position=idx):
        with open(file_path, 'rb') as file:
            file_data = file.read()
            if len(func(file_data, pattern)):
                results.append(file_path)
    return results

def load_virus_patterns(virus_folder):
    virus_patterns = {}
    for virus_file in sorted(os.listdir(virus_folder)):
        virus_path = os.path.join(virus_folder, virus_file)
        try:
            with open(virus_path, 'rb') as file:
                virus_patterns[virus_file] = file.read()
        except Exception as e:
            print(f"无法读取病毒文件 {virus_path}: {e}")
    return virus_patterns

def scan_software_for_viruses(base_folder, virus_folder, result_file_path, match_type):
    virus_patterns = load_virus_patterns(virus_folder)

    # 获取所有文件路径
    files_to_scan = []
    for root, _, files in os.walk(base_folder):
        for file in files:
            file_path = os.path.join(root, file)
            files_to_scan.append(file_path)

    results = dict()
    # 分别对10个病毒文件进行扫描
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        futures = dict()
        idx = 0
        for virus_name, virus_pattern in virus_patterns.items():
            futures[executor.submit(match_viruses_in_files, files_to_scan, virus_pattern, idx, match_type)] = virus_name
            idx += 1
        for future in concurrent.futures.as_completed(futures):
            for file in future.result():
                if file not in results:
                    results[file] = []
                results[file].append(futures[future])
    
    print(f"使用算法{match_type}扫描结束，共耗时{time.time() - start_time:.2f}s")

    if match_type == 'match_pattern':
        # 保存结果（只存一次即可）
        with open(result_file_path, 'w', encoding='utf-8') as result_file:
            for file_path, matched_viruses in results.items():
                cleaned_file_path = file_path.lstrip(os.sep)
                while cleaned_file_path.startswith(".."):
                    cleaned_file_path = cleaned_file_path[3:]  # 特殊处理，删除 ../ 前缀

                virus_list_str = " ".join(sorted(matched_viruses))
                result_file.write(f"{cleaned_file_path} {virus_list_str}\n")

    
if __name__ == "__main__":
    # 配置路径
    base_folder = '../data/software_antivirus/opencv-4.10.0'
    virus_folder = '../data/software_antivirus/virus'
    result_file_path = '../result_software.txt'

    # 扫描病毒并保存结果
    time1 = time.time()
    scan_software_for_viruses(base_folder, virus_folder, result_file_path, 'match_pattern')
    scan_software_for_viruses(base_folder, virus_folder, result_file_path, 'kmp_search')
    scan_software_for_viruses(base_folder, virus_folder, result_file_path, 'par_kmp_search')
    print(f"Time: {time.time() - time1:.2f}s")
