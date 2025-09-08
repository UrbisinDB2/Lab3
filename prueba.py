import os, struct, bisect

BLOCK_FACTOR = 3


# ---------------- Record ----------------
class Record:
    # Formato coherente con el pack (id, nombre(40), cantidad, precio(float), fecha(15))
    FORMAT = 'i40sif15s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id: int, nombre: str, cantidad: int, precio: float, fecha: str):
        self.id = id
        self.nombre = (nombre or "")[:40]
        self.cantidad = int(cantidad)
        self.precio = float(precio)
        self.fecha = (fecha or "")[:15]

    def pack(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.id,
            self.nombre.encode().ljust(40, b'\x00'),
            self.cantidad,
            self.precio,
            self.fecha.encode().ljust(15, b'\x00')
        )

    @staticmethod
    def unpack(data: bytes):
        id, nombre, cantidad, precio, fecha = struct.unpack(Record.FORMAT, data)
        return Record(
            id,
            nombre.decode(errors="ignore").rstrip('\x00').strip(),
            cantidad,
            precio,
            fecha.decode(errors="ignore").rstrip('\x00').strip()
        )

    def __repr__(self):
        return f"Record({self.id}, '{self.nombre}', {self.cantidad}, {self.precio}, '{self.fecha}')"


# ---------------- Page ----------------
class Page:
    # header: size, next_page  (next_page = -1 si no hay)
    FORMAT_HEADER = 'ii'
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_PAGE = SIZE_HEADER + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=None, next_page=-1):
        self.records = records or []
        self.next_page = next_page  # índice de página (0-based) o -1

    def pack(self) -> bytes:
        buf = struct.pack(self.FORMAT_HEADER, len(self.records), self.next_page)
        for r in self.records:
            buf += r.pack()
        # padding
        remaining = BLOCK_FACTOR - len(self.records)
        buf += b'\x00' * (remaining * Record.SIZE_OF_RECORD)
        return buf

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(Page.FORMAT_HEADER, data[:Page.SIZE_HEADER])
        recs, off = [], Page.SIZE_HEADER
        for _ in range(size):
            recs.append(Record.unpack(data[off:off + Record.SIZE_OF_RECORD]))
            off += Record.SIZE_OF_RECORD
        return Page(recs, next_page)


# ---------------- Data + Index ----------------
class DataFile:
    def __init__(self, file_name):
        self.file_name = file_name

    # helpers de E/S de páginas
    def _num_pages(self, f):
        f.seek(0, 2)
        return (f.tell() // Page.SIZE_OF_PAGE)

    def _read_page(self, f, page_no: int) -> Page:
        f.seek(page_no * Page.SIZE_OF_PAGE, 0)
        return Page.unpack(f.read(Page.SIZE_OF_PAGE))

    def _write_page(self, f, page_no: int, page: Page):
        f.seek(page_no * Page.SIZE_OF_PAGE, 0)
        f.write(page.pack())

    def _append_page(self, f, page: Page) -> int:
        """Escribe una página al final. Retorna su número de página."""
        pno = self._num_pages(f)
        f.seek(pno * Page.SIZE_OF_PAGE, 0)
        f.write(page.pack())
        return pno


class IndexFile:
    # clave mínima de página primaria + número de página
    FORMAT = 'ii'
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, file_name):
        self.file_name = file_name

    def build(self, data_path: str):
        if not os.path.exists(data_path):
            return
        with open(data_path, 'rb') as df, open(self.file_name, 'wb') as ix:
            n = (os.path.getsize(data_path) // Page.SIZE_OF_PAGE)
            for pno in range(n):
                pg = Page.unpack(df.read(Page.SIZE_OF_PAGE) if pno == 0 else
                                 (df.seek(pno * Page.SIZE_OF_PAGE, 0) or df.read(Page.SIZE_OF_PAGE)))
                if pg.records:
                    first_key = pg.records[0].id
                    ix.write(struct.pack(self.FORMAT, first_key, pno))

    def _load_all(self):
        entries = []
        if not os.path.exists(self.file_name):
            return entries
        with open(self.file_name, 'rb') as f:
            while chunk := f.read(self.SIZE):
                entries.append(struct.unpack(self.FORMAT, chunk))  # (key, pno)
        return entries

    def find_page_for_key(self, key: int) -> int | None:
        """Devuelve la página primaria donde debería caer la clave."""
        entries = self._load_all()
        if not entries:
            return 0  # si no hay índice, asumimos página 0 (se creará)
        # buscamos el mayor key <= buscada
        best = None
        for k, p in entries:
            if k <= key:
                best = p
            else:
                break
        return best if best is not None else entries[0][1]

    def update_on_split(self, old_first_key: int, new_first_key: int, new_page_no: int):
        """Solo si la nueva página va a ser primaria. Para overflow clásico NO hace falta."""
        # En ISAM clásico con overflow encadenado NO cambiamos el índice,
        # porque sigue apuntando a la página primaria. Dejo la función por si luego decides promover.
        pass


# ---------------- ISAM con split + overflow encadenado ----------------
class ISAM:
    def __init__(self, data_path='data.dat', index_path='index.dat'):
        self.data = DataFile(data_path)
        self.index = IndexFile(index_path)
        # crea archivo si no existe
        if not os.path.exists(data_path):
            with open(data_path, 'wb') as f:
                pass

    def _insert_into_page_sorted(self, page: Page, rec: Record):
        # Inserta manteniendo ORDEN LOCAL de esa página (no toca otras páginas)
        keys = [r.id for r in page.records]
        pos = bisect.bisect_left(keys, rec.id)
        page.records.insert(pos, rec)

    def insert(self, rec: Record):
        # 1) localizar página primaria por índice
        target = self.index.find_page_for_key(rec.id)
        with open(self.data.file_name, 'r+b') as f:
            # si no hay páginas, crea una primaria
            if self.data._num_pages(f) == 0:
                self.data._append_page(f, Page([rec], -1))
                # índice inicial
                with open(self.index.file_name, 'wb') as ix:
                    ix.write(struct.pack(self.index.FORMAT, rec.id, 0))
                return

            # 2) recorrer chain: primaria -> overflows hasta ubicar lugar
            cur_no = target
            while True:
                cur_pg = self.data._read_page(f, cur_no)

                # si la clave cae antes que el primer registro de esta página y es PRIMARIA,
                # podrías (opcional) insertar aquí para mantener mínima. (lo normal: cae aquí igual)
                self._insert_into_page_sorted(cur_pg, rec)

                if len(cur_pg.records) <= BLOCK_FACTOR:
                    # hay espacio: escribir y listo
                    self.data._write_page(f, cur_no, cur_pg)
                    break
                else:
                    # 3) página llena -> split en DOS sin tocar el resto del archivo
                    #    mitad "baja" se queda; mitad "alta" va a nueva página encadenada
                    #    garantizamos que la primera página conserva su mínima (índice no cambia)
                    mid = (len(cur_pg.records) + 1) // 2  # sesgo para que primaria conserve mínimas
                    low = cur_pg.records[:mid]
                    high = cur_pg.records[mid:]

                    # reescribir la página actual con 'low'
                    cur_pg.records = low

                    # nueva página apunta al siguiente del chain
                    new_pg = Page(high, cur_pg.next_page)
                    new_no = self.data._append_page(f, new_pg)

                    # encadenar
                    cur_pg.next_page = new_no
                    self.data._write_page(f, cur_no, cur_pg)

                    # Si el registro insertado quedó en 'high', ya está en new_pg y terminamos.
                    # Si no, también terminamos: el split local resolvió la sobrecarga.
                    break

            # 4) reconstruir índice solo si no existe (o si quieres, lo puedes rebuild al final del batch)
            if not os.path.exists(self.index.file_name) or os.path.getsize(self.index.file_name) == 0:
                self.index.build(self.data.file_name)

    # Búsqueda: índice -> primaria -> overflow encadenado
    def search(self, key: int):
        pno = self.index.find_page_for_key(key)
        if pno is None:
            return None
        with open(self.data.file_name, 'rb') as f:
            while pno != -1:
                pg = self.data._read_page(f, pno)
                for r in pg.records:
                    if r.id == key:
                        return r
                pno = pg.next_page
        return None

    # solo para debug
    def scanAll(self):
        with open(self.data.file_name, 'rb') as f:
            n = self.data._num_pages(f)
            for i in range(n):
                pg = self.data._read_page(f, i)
                chain = f"(next={pg.next_page})"
                print(f"-- Page {i} {chain}")
                for r in pg.records:
                    print("   ", r)
        print("== Index ==")
        for k, p in self.index._load_all():
            print(f"  key_min={k} -> page {p}")

isam = ISAM('data.dat', 'index.dat')

# Inserciones desordenadas: NO reescribe todo, solo divide la página si se llena
isam.insert(Record(10, "A", 1, 1.0, "2024-01-01"))
isam.insert(Record(2,  "B", 2, 2.0, "2024-01-02"))
isam.insert(Record(7,  "C", 3, 3.0, "2024-01-03"))
isam.insert(Record(1,  "D", 4, 4.0, "2024-01-04"))
isam.insert(Record(12, "E", 5, 5.0, "2024-01-05"))
isam.insert(Record(5,  "F", 6, 6.0, "2024-01-06"))
isam.insert(Record(6,  "G", 7, 7.0, "2024-01-07"))
isam.scanAll()
