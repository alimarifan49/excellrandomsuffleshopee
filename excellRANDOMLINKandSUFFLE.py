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

LINK_COLUMNS = ["link2", "link3", "link4", "link5", "link6"]

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
        df_pending = df[df['status'] == 'pending']

        if df_pending.empty:
            messagebox.showinfo("Info", "Tidak ada pending")
            return

        df_pending_new = smart_shuffle(df_pending)
        df_final = pd.concat([df_done, df_pending_new], ignore_index=True)

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
# GUI
# ==============================
root = tk.Tk()
root.title("Shopee Tool 2 Mode")
root.geometry("450x300")

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

# STATUS
status_label = tk.Label(root, text="Status: Idle", fg="blue")
status_label.pack(pady=10)

root.mainloop()