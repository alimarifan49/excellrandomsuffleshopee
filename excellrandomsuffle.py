import pandas as pd
import random
import tkinter as tk
from tkinter import filedialog, messagebox
import threading
import os

file_path = ""

# ==============================
# PILIH FILE
# ==============================
def pilih_file():
    global file_path
    file_path = filedialog.askopenfilename(
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if file_path:
        label_file.config(text=f"File: {os.path.basename(file_path)}")


# ==============================
# PROSES (THREAD)
# ==============================
def proses_thread():
    global file_path

    try:
        status_label.config(text="Processing...")
        btn_proses.config(state="disabled")

        df = pd.read_excel(file_path)

        # ==============================
        # VALIDASI KOLOM
        # ==============================
        required_cols = ['video', 'link', 'caption', 'status', 'nama produk']
        for col in required_cols:
            if col not in df.columns:
                messagebox.showerror("Error", f"Kolom '{col}' tidak ditemukan!")
                reset_ui()
                return

        # ==============================
        # FILTER DATA
        # ==============================
        df_done = df[df['status'].str.lower() == 'done']
        df_pending = df[df['status'].str.lower() == 'pending']

        if df_pending.empty:
            messagebox.showinfo("Info", "Tidak ada data pending.")
            reset_ui()
            return

        # ==============================
        # GROUPING (LEBIH CEPAT)
        # ==============================
        groups = {}

        for row in df_pending.to_dict('records'):
            key = row['nama produk']
            groups.setdefault(key, []).append(row)

        # shuffle tiap grup
        for key in groups:
            random.shuffle(groups[key])

        # ==============================
        # ROUND ROBIN (ANTI INFINITE LOOP)
        # ==============================
        result = []
        last_product = None

        while any(groups.values()):
            progress = False

            for key in list(groups.keys()):
                if groups[key] and key != last_product:
                    item = groups[key].pop(0)
                    result.append(item)
                    last_product = key
                    progress = True

            # 🚨 FIX: kalau stuck → paksa ambil
            if not progress:
                for key in list(groups.keys()):
                    if groups[key]:
                        item = groups[key].pop(0)
                        result.append(item)
                        last_product = key
                        break

        df_pending_new = pd.DataFrame(result)

        # ==============================
        # GABUNGKAN
        # ==============================
        df_final = pd.concat([df_done, df_pending_new], ignore_index=True)

        # ==============================
        # SIMPAN
        # ==============================
        save_path = file_path.replace(".xlsx", "_hasil.xlsx")
        df_final.to_excel(save_path, index=False)

        status_label.config(text="Selesai ✅")
        messagebox.showinfo("Sukses", f"File berhasil dibuat:\n{save_path}")

    except Exception as e:
        status_label.config(text="Error ❌")
        messagebox.showerror("Error", str(e))

    finally:
        reset_ui()


# ==============================
# RESET UI
# ==============================
def reset_ui():
    btn_proses.config(state="normal")


# ==============================
# TRIGGER THREAD
# ==============================
def proses_data():
    global file_path

    if not file_path:
        messagebox.showwarning("Warning", "Pilih file Excel dulu!")
        return

    thread = threading.Thread(target=proses_thread)
    thread.start()


# ==============================
# GUI
# ==============================
root = tk.Tk()
root.title("Shopee Video Optimizer")
root.geometry("420x260")

title = tk.Label(root, text="Smart Shuffle Video Shopee", font=("Arial", 14, "bold"))
title.pack(pady=10)

btn_pilih = tk.Button(root, text="Pilih File Excel", command=pilih_file, width=25)
btn_pilih.pack(pady=5)

label_file = tk.Label(root, text="Belum ada file dipilih", fg="gray")
label_file.pack(pady=5)

btn_proses = tk.Button(root, text="Proses", command=proses_data, bg="green", fg="white", width=20)
btn_proses.pack(pady=15)

status_label = tk.Label(root, text="Status: Idle", fg="blue")
status_label.pack(pady=10)

root.mainloop()