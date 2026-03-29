import pandas as pd
import random
import tkinter as tk
from tkinter import filedialog, messagebox
import threading
import os
from collections import defaultdict
import math

file_path_link = ""
file_path_shuffle = ""
file_path_restore_reference = ""
file_path_restore_current = ""

LINK_COLUMNS = ["link2", "link3", "link4", "link5", "link6"]


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_status(series):
    return series.fillna("").astype(str).str.lower().str.strip()


def build_occurrence_keys(df, key_columns):
    counts = defaultdict(int)
    keys = []

    for row in df[key_columns].itertuples(index=False, name=None):
        base_key = tuple(normalize_text(value) for value in row)
        counts[base_key] += 1
        keys.append(base_key + (counts[base_key],))

    return keys

# ==============================
# PILIH FILE (LINK)
# ==============================
def pilih_file_link():
    global file_path_link
    file_path_link = filedialog.askopenfilename(
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if file_path_link:
        label_file_link.config(text=os.path.basename(file_path_link))


# ==============================
# PILIH FILE (SHUFFLE)
# ==============================
def pilih_file_shuffle():
    global file_path_shuffle
    file_path_shuffle = filedialog.askopenfilename(
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if file_path_shuffle:
        label_file_shuffle.config(text=os.path.basename(file_path_shuffle))


# ==============================
# PILIH FILE (RESTORE REFERENSI A)
# ==============================
def pilih_file_restore_reference():
    global file_path_restore_reference
    file_path_restore_reference = filedialog.askopenfilename(
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if file_path_restore_reference:
        label_file_restore_reference.config(text=os.path.basename(file_path_restore_reference))


# ==============================
# PILIH FILE (RESTORE FILE B)
# ==============================
def pilih_file_restore_current():
    global file_path_restore_current
    file_path_restore_current = filedialog.askopenfilename(
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if file_path_restore_current:
        label_file_restore_current.config(text=os.path.basename(file_path_restore_current))


# ==============================
# ISI LINK
# ==============================
def isi_link(df):
    df = df.copy()

    for idx, row in df.iterrows():

        if row["status"] != "pending":
            continue

        kategori = row["nama produk"]

        same_category = df[df["nama produk"] == kategori]
        same_category = same_category.drop(idx)

        links = same_category["link1"].tolist()
        links = [l for l in links if l]

        random.shuffle(links)

        selected = links[:len(LINK_COLUMNS)]

        for i, col in enumerate(LINK_COLUMNS):
            df.at[idx, col] = selected[i] if i < len(selected) else ""

    return df


# ==============================
# SMART SHUFFLE
# ==============================
def smart_shuffle(df_pending):

    groups = defaultdict(list)

    for row in df_pending.to_dict('records'):
        groups[row['nama produk']].append(row)

    for key in groups:
        random.shuffle(groups[key])

    result = []

    while any(groups.values()):
        batch = []

        for key in list(groups.keys()):
            items = groups[key]
            n = len(items)

            if n == 0:
                continue

            if n >= 5:
                percent = 0.20
            elif n == 4:
                percent = 0.25
            elif n == 3:
                percent = 0.30
            else:
                percent = 0.50

            take = max(1, math.ceil(n * percent))

            for _ in range(min(take, len(items))):
                batch.append(items.pop(0))

        random.shuffle(batch)
        result.extend(batch)

    return pd.DataFrame(result)


def reorder_pending_by_reference(df_reference, df_current):
    df_reference = df_reference.copy()
    df_current = df_current.copy()

    df_reference["status"] = normalize_status(df_reference["status"])
    df_current["status"] = normalize_status(df_current["status"])

    common_key_columns = [
        col for col in df_reference.columns
        if col in df_current.columns and col != "status"
    ]

    if not common_key_columns:
        raise ValueError("File A dan File B tidak punya kolom identitas yang sama selain status.")

    df_reference["_restore_key"] = build_occurrence_keys(df_reference, common_key_columns)
    df_current["_restore_key"] = build_occurrence_keys(df_current, common_key_columns)

    df_done = df_current[df_current["status"] == "done"].copy()
    df_other = df_current[
        (df_current["status"] != "done") &
        (df_current["status"] != "pending")
    ].copy()
    df_pending = df_current[df_current["status"] == "pending"].copy()

    pending_lookup = {
        key: row_index
        for row_index, key in zip(df_pending.index, df_pending["_restore_key"])
    }

    ordered_pending_indexes = []
    used_keys = set()

    for key in df_reference.loc[df_reference["status"] == "pending", "_restore_key"]:
        row_index = pending_lookup.get(key)
        if row_index is not None:
            ordered_pending_indexes.append(row_index)
            used_keys.add(key)

    ordered_pending = df_current.loc[ordered_pending_indexes].copy()
    leftover_pending = df_pending[~df_pending["_restore_key"].isin(used_keys)].copy()

    final_frames = [
        df_done.drop(columns=["_restore_key"]),
        df_other.drop(columns=["_restore_key"]),
        ordered_pending.drop(columns=["_restore_key"]),
        leftover_pending.drop(columns=["_restore_key"]),
    ]
    df_final = pd.concat(final_frames, ignore_index=True)

    restore_info = {
        "matched_pending": len(ordered_pending),
        "leftover_pending": len(leftover_pending),
        "key_columns": common_key_columns,
    }
    return df_final, restore_info


# ==============================
# PROSES LINK
# ==============================
def proses_link_thread():
    try:
        status_label.config(text="Processing Link...")
        btn_proses_link.config(state="disabled")

        df = pd.read_excel(file_path_link, dtype=str).fillna("")

        required_cols = ['link1', 'status', 'nama produk']
        for col in required_cols:
            if col not in df.columns:
                messagebox.showerror("Error", f"Kolom '{col}' tidak ditemukan!")
                return

        df["status"] = df["status"].str.lower().str.strip()
        df["nama produk"] = df["nama produk"].str.strip()
        df["link1"] = df["link1"].str.strip()

        for col in LINK_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        df = isi_link(df)

        save_path = file_path_link.replace(".xlsx", "_link.xlsx")
        df.to_excel(save_path, index=False)

        messagebox.showinfo("Sukses", f"Berhasil:\n{save_path}")
        status_label.config(text="Selesai Link ✅")

    except Exception as e:
        messagebox.showerror("Error", str(e))
        status_label.config(text="Error ❌")

    finally:
        btn_proses_link.config(state="normal")


def proses_link():
    if not file_path_link:
        messagebox.showwarning("Warning", "Pilih file dulu!")
        return
    threading.Thread(target=proses_link_thread).start()


# ==============================
# PROSES SHUFFLE
# ==============================
def proses_shuffle_thread():
    try:
        status_label.config(text="Processing Shuffle...")
        btn_proses_shuffle.config(state="disabled")

        df = pd.read_excel(file_path_shuffle, dtype=str).fillna("")

        required_cols = ['status', 'nama produk']
        for col in required_cols:
            if col not in df.columns:
                messagebox.showerror("Error", f"Kolom '{col}' tidak ditemukan!")
                return

        df["status"] = df["status"].str.lower().str.strip()
        df["nama produk"] = df["nama produk"].str.strip()

        df_done = df[df['status'] == 'done']
        df_other = df[(df['status'] != 'done') & (df['status'] != 'pending')]
        df_pending = df[df['status'] == 'pending']

        if df_pending.empty:
            messagebox.showinfo("Info", "Tidak ada pending")
            return

        df_pending_new = smart_shuffle(df_pending)
        df_final = pd.concat([df_done, df_other, df_pending_new], ignore_index=True)

        save_path = file_path_shuffle.replace(".xlsx", "_shuffle.xlsx")
        df_final.to_excel(save_path, index=False)

        messagebox.showinfo("Sukses", f"Berhasil:\n{save_path}")
        status_label.config(text="Selesai Shuffle ✅")

    except Exception as e:
        messagebox.showerror("Error", str(e))
        status_label.config(text="Error ❌")

    finally:
        btn_proses_shuffle.config(state="normal")


def proses_shuffle():
    if not file_path_shuffle:
        messagebox.showwarning("Warning", "Pilih file dulu!")
        return
    threading.Thread(target=proses_shuffle_thread).start()


# ==============================
# PROSES RESTORE B -> A
# ==============================
def proses_restore_thread():
    try:
        status_label.config(text="Processing Restore...")
        btn_proses_restore.config(state="disabled")

        df_reference = pd.read_excel(file_path_restore_reference, dtype=str).fillna("")
        df_current = pd.read_excel(file_path_restore_current, dtype=str).fillna("")

        required_cols = ["status", "nama produk"]
        for col in required_cols:
            if col not in df_reference.columns:
                messagebox.showerror("Error", f"Kolom '{col}' tidak ditemukan di File A!")
                return
            if col not in df_current.columns:
                messagebox.showerror("Error", f"Kolom '{col}' tidak ditemukan di File B!")
                return

        df_reference["nama produk"] = df_reference["nama produk"].astype(str).str.strip()
        df_current["nama produk"] = df_current["nama produk"].astype(str).str.strip()

        df_final, restore_info = reorder_pending_by_reference(df_reference, df_current)

        save_path = file_path_restore_current.replace(".xlsx", "_restore.xlsx")
        df_final.to_excel(save_path, index=False)

        message = (
            f"Berhasil restore urutan pending.\n\n"
            f"Pending cocok: {restore_info['matched_pending']}\n"
            f"Pending tidak cocok: {restore_info['leftover_pending']}\n"
            f"File hasil:\n{save_path}"
        )
        messagebox.showinfo("Sukses", message)
        status_label.config(text="Selesai Restore ✅")

    except Exception as e:
        messagebox.showerror("Error", str(e))
        status_label.config(text="Error ❌")

    finally:
        btn_proses_restore.config(state="normal")


def proses_restore():
    if not file_path_restore_reference:
        messagebox.showwarning("Warning", "Pilih File A dulu!")
        return
    if not file_path_restore_current:
        messagebox.showwarning("Warning", "Pilih File B dulu!")
        return
    threading.Thread(target=proses_restore_thread).start()


def main():
    global root
    global btn_proses_link
    global label_file_link
    global btn_proses_shuffle
    global label_file_shuffle
    global btn_proses_restore
    global label_file_restore_reference
    global label_file_restore_current
    global status_label

    # ==============================
    # GUI
    # ==============================
    root = tk.Tk()
    root.title("Shopee Tool 2 Mode")
    root.geometry("500x470")

    title = tk.Label(root, text="Shopee Automation Tool", font=("Arial", 14, "bold"))
    title.pack(pady=10)

    # ===== LINK =====
    frame1 = tk.LabelFrame(root, text="Random Link")
    frame1.pack(fill="x", padx=10, pady=5)

    btn_file_link = tk.Button(frame1, text="Pilih File", command=pilih_file_link)
    btn_file_link.pack(pady=3)

    label_file_link = tk.Label(frame1, text="Belum ada file", fg="gray")
    label_file_link.pack()

    btn_proses_link = tk.Button(frame1, text="Proses Link", bg="green", fg="white", command=proses_link)
    btn_proses_link.pack(pady=5)

    # ===== SHUFFLE =====
    frame2 = tk.LabelFrame(root, text="Smart Shuffle")
    frame2.pack(fill="x", padx=10, pady=5)

    btn_file_shuffle = tk.Button(frame2, text="Pilih File", command=pilih_file_shuffle)
    btn_file_shuffle.pack(pady=3)

    label_file_shuffle = tk.Label(frame2, text="Belum ada file", fg="gray")
    label_file_shuffle.pack()

    btn_proses_shuffle = tk.Button(frame2, text="Proses Shuffle", bg="blue", fg="white", command=proses_shuffle)
    btn_proses_shuffle.pack(pady=5)

    # ===== RESTORE =====
    frame3 = tk.LabelFrame(root, text="Restore B ke A (Sedia Kala)")
    frame3.pack(fill="x", padx=10, pady=5)

    btn_file_restore_reference = tk.Button(frame3, text="Pilih File A", command=pilih_file_restore_reference)
    btn_file_restore_reference.pack(pady=3)

    label_file_restore_reference = tk.Label(frame3, text="Belum ada File A", fg="gray")
    label_file_restore_reference.pack()

    btn_file_restore_current = tk.Button(frame3, text="Pilih File B", command=pilih_file_restore_current)
    btn_file_restore_current.pack(pady=3)

    label_file_restore_current = tk.Label(frame3, text="Belum ada File B", fg="gray")
    label_file_restore_current.pack()

    btn_proses_restore = tk.Button(frame3, text="Proses Restore", bg="orange", fg="white", command=proses_restore)
    btn_proses_restore.pack(pady=5)

    # STATUS
    status_label = tk.Label(root, text="Status: Idle", fg="blue")
    status_label.pack(pady=10)

    root.mainloop()


if __name__ == "__main__":
    main()