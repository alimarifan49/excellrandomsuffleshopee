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
file_path_sinkron_utama = ""
file_path_sinkron_shuffle_ref = ""

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
# SINKRONISASI STATUS VIDEO
# ==============================

def validate_columns(df, required, label):
    missing = [col for col in required if col not in df.columns]
    if missing:
        messagebox.showerror(
            "Kolom Tidak Ditemukan",
            f"File {label} tidak memiliki kolom: {', '.join(missing)}"
        )
        return False
    return True


def proses_sinkronisasi(path_utama, path_shuffle):
    REQUIRED = ["video", "status"]

    df_utama = pd.read_excel(path_utama)
    df_shuffle = pd.read_excel(path_shuffle)

    if not validate_columns(df_utama, REQUIRED, "DATAUTAMA"):
        return None
    if not validate_columns(df_shuffle, REQUIRED, "shuffle"):
        return None

    df_utama["_nama_key"] = df_utama["video"].astype(str).str.strip().str.lower()
    df_shuffle["_nama_key"] = df_shuffle["video"].astype(str).str.strip().str.lower()
    df_shuffle["_status_lower"] = df_shuffle["status"].astype(str).str.strip().str.lower()

    done_set = set(
        df_shuffle.loc[df_shuffle["_status_lower"] == "done", "_nama_key"]
    )

    mask_pending = df_utama["status"].astype(str).str.strip().str.lower() == "pending"
    mask_match = df_utama["_nama_key"].isin(done_set)
    jumlah_berubah = int((mask_pending & mask_match).sum())

    df_utama.loc[mask_match, "status"] = "done"
    df_utama.drop(columns=["_nama_key"], inplace=True)

    urutan_status = {"done": 0, "pending": 1}
    df_utama["_sort_key"] = (
        df_utama["status"].astype(str).str.strip().str.lower()
        .map(urutan_status)
        .fillna(2)
    )
    df_utama["_video_lower"] = df_utama["video"].astype(str).str.strip().str.lower()
    df_utama.sort_values(["_sort_key", "_video_lower"], inplace=True)
    df_utama.drop(columns=["_sort_key", "_video_lower"], inplace=True)
    df_utama.reset_index(drop=True, inplace=True)

    folder_output = os.path.dirname(path_utama)
    path_output = os.path.join(folder_output, "DATAUTAMA_UPDATED.xlsx")
    df_utama.to_excel(path_output, index=False)

    return (
        f"Proses selesai!\n\n"
        f"Data diperbarui : {jumlah_berubah} baris (pending \u2192 done)\n"
        f"File disimpan   : {path_output}"
    )


def pilih_file_utama():
    global file_path_sinkron_utama
    file_path_sinkron_utama = filedialog.askopenfilename(
        title="Pilih File Data Utama",
        filetypes=[("Excel Files", "*.xlsx *.xls"), ("All files", "*.*")]
    )
    if file_path_sinkron_utama:
        label_sinkron_utama.config(
            text=os.path.basename(file_path_sinkron_utama), fg="#2e7d32"
        )


def pilih_sinkron_shuffle():
    global file_path_sinkron_shuffle_ref
    file_path_sinkron_shuffle_ref = filedialog.askopenfilename(
        title="Pilih File Shuffle (POST)",
        filetypes=[("Excel Files", "*.xlsx *.xls"), ("All files", "*.*")]
    )
    if file_path_sinkron_shuffle_ref:
        label_sinkron_shuffle.config(
            text=os.path.basename(file_path_sinkron_shuffle_ref), fg="#2e7d32"
        )


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
# PROSES SINKRONISASI
# ==============================
def jalankan_proses():
    if not file_path_sinkron_utama:
        messagebox.showwarning("File Belum Dipilih", "Silakan pilih file Data Utama terlebih dahulu.")
        return
    if not file_path_sinkron_shuffle_ref:
        messagebox.showwarning("File Belum Dipilih", "Silakan pilih file Shuffle terlebih dahulu.")
        return
    if not os.path.isfile(file_path_sinkron_utama):
        messagebox.showerror("File Tidak Ditemukan", f"File tidak ditemukan:\n{file_path_sinkron_utama}")
        return
    if not os.path.isfile(file_path_sinkron_shuffle_ref):
        messagebox.showerror("File Tidak Ditemukan", f"File tidak ditemukan:\n{file_path_sinkron_shuffle_ref}")
        return

    lbl_sinkron_status.config(text="Memproses...", fg="#f57c00")
    root.update_idletasks()

    try:
        pesan = proses_sinkronisasi(file_path_sinkron_utama, file_path_sinkron_shuffle_ref)
        if pesan:
            lbl_sinkron_status.config(text="Selesai!", fg="#2e7d32")
            messagebox.showinfo("Sukses", pesan)
    except Exception as e:
        lbl_sinkron_status.config(text="Terjadi kesalahan.", fg="#c62828")
        messagebox.showerror("Error", f"Terjadi kesalahan:\n{str(e)}")


def main():
    global root
    global btn_proses_link
    global label_file_link
    global btn_proses_shuffle
    global label_file_shuffle
    global label_sinkron_utama
    global label_sinkron_shuffle
    global lbl_sinkron_status
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

    # ===== SINKRONISASI STATUS VIDEO =====
    frame3 = tk.LabelFrame(root, text="Sinkronisasi Status Video Excel")
    frame3.pack(fill="x", padx=10, pady=5)

    btn_file_sinkron_utama = tk.Button(frame3, text="Pilih File Data Utama", command=pilih_file_utama)
    btn_file_sinkron_utama.pack(pady=3)

    label_sinkron_utama = tk.Label(frame3, text="Belum dipilih", fg="gray")
    label_sinkron_utama.pack()

    btn_file_sinkron_shuffle = tk.Button(frame3, text="Pilih File Shuffle (POST)", command=pilih_sinkron_shuffle)
    btn_file_sinkron_shuffle.pack(pady=3)

    label_sinkron_shuffle = tk.Label(frame3, text="Belum dipilih", fg="gray")
    label_sinkron_shuffle.pack()

    btn_proses_sinkron = tk.Button(frame3, text="▶  Proses Sinkronisasi", bg="purple", fg="white", command=jalankan_proses)
    btn_proses_sinkron.pack(pady=5)

    lbl_sinkron_status = tk.Label(frame3, text="Menunggu...", fg="#757575")
    lbl_sinkron_status.pack(pady=(0, 5))

    # STATUS
    status_label = tk.Label(root, text="Status: Idle", fg="blue")
    status_label.pack(pady=10)

    root.mainloop()


if __name__ == "__main__":
    main()