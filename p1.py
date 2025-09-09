import struct, os, csv
from math import floor

class Record:
    FORMAT = 'i40sif15s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id: int, nombre: str, cantidad: int, precio: float, fecha: str):
        self.id = id
        self.nombre = nombre
        self.cantidad = cantidad
        self.precio = precio
        self.fecha = fecha

    def pack(self) -> bytes:
        return struct.pack(self.FORMAT,
        self.id, self.nombre[:30].ljust(20).encode(),
        self.cantidad, self.precio, self.fecha[:15].ljust(15).encode()
        )

    @staticmethod
    def unpack(data: bytes):
        id, nombre, cantidad, precio, fecha = struct.unpack(Record.FORMAT, data)
        return Record(id, nombre.decode().rstrip(), cantidad, precio, fecha.decode().rstrip())

    def __str__(self):
        return (str(self.id) + " | " + self.nombre + " | " + str(self.cantidad) + " | " +
                str(self.precio) + " | " + str(self.fecha))

BUFFER_SIZE = 1024
BLOCK_FACTOR = 23
INDEX_FACTOR = 127

class Page:
    FORMAT_HEADER = 'ii' #size, next_page
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_PAGE = SIZE_HEADER + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records = [], next_page = -1):
        self.records = records
        self.next_page = next_page

    def pack(self) -> bytes:
        # 1- empaquetar el size y el next_page
        header_data = struct.pack(self.FORMAT_HEADER, len(self.records), self.next_page)
        record_data = b''
        for record in self.records:
            record_data += record.pack()
        i = len(self.records)
        while i < BLOCK_FACTOR:
            record_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1
        return header_data + record_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(Page.FORMAT_HEADER, data[:Page.SIZE_HEADER])
        offset = Page.SIZE_HEADER
        records = []
        for i in range(size):
            record_data = data[offset : offset + Record.SIZE_OF_RECORD]
            record = Record.unpack(record_data)
            records.append(record)
            offset += Record.SIZE_OF_RECORD
        return Page(records, next_page)

    def position(self, record_id: int) -> int | None:
        left, right = 0, len(self.records) - 1

        while left <= right:
            mid = (left + right) // 2
            mid_id = self.records[mid].id  # asumiendo que Record tiene atributo .id

            if mid_id == record_id:
                # Ya existe, terminamos la inserción
                return None
            elif mid_id < record_id:
                left = mid + 1
            else:
                right = mid - 1

        # Si no existe, retornamos la posición de inserción
        return left


class IndexFile:
    FORMAT_HEADER = 'i'
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_INDEX = SIZE_HEADER + struct.calcsize('i') * INDEX_FACTOR + struct.calcsize('i') * (INDEX_FACTOR + 1)

    def __init__(self, file_name: str, pages = [], keys = []):
        self.file_name = file_name
        self.pages = pages
        self.keys = keys

    def getIndex(self):
        pages = []
        keys = []
        with open(self.file_name, 'rb') as file:
            size = struct.unpack('i', file.read(4))[0]

            if size == 1:
                file.seek(4)
                p0 = struct.unpack('i', file.read(4))[0]
                pages.append(p0)
            else:
                file.seek(4)
                p0 = struct.unpack('i', file.read(4))[0]
                pages.append(p0)

                for i in range(1, size):
                    ki = struct.unpack('i', file.read(4))[0]
                    keys.append(ki)
                    pi = struct.unpack('i', file.read(4))[0]
                    pages.append(pi)

        return pages, keys


    def addIndex(self, page_pos: int, key: int):
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'wb') as file:
                file.write(struct.pack(self.FORMAT_HEADER, 1))
                file.write(struct.pack('i', page_pos))
            return

        with open(self.file_name, 'r+b') as file:
            file.seek(0, 2)
            file.write(struct.pack('i',key))
            file.write(struct.pack('i', page_pos))
            file.seek(0)
            size = struct.unpack('i', file.read(4))[0]
            size += 1
            file.seek(0)
            file.write(struct.pack(self.FORMAT_HEADER, size))

    def updateIndex(self, page_pos: int, key: int) -> bool:
        if not os.path.exists(self.file_name):
            return False

        # 1) Cargar índice en memoria
        try:
            pages, keys = self.getIndex()  # pages = [p0, p1, ..., p_{m-1}], keys = [k1, ..., k_{m-1}]
        except FileNotFoundError:
            return False

        if len(pages) == 0:
            return False  # índice corrupto

        # 2) Buscar posición de inserción con upper_bound (después de duplicados)
        #    pos ∈ [0 .. len(keys)]
        left, right = 0, len(keys)  # nota: right es "one past the end" para upper_bound
        while left < right:
            mid = (left + right) // 2
            if keys[mid] <= key:
                left = mid + 1
            else:
                right = mid
        pos = left  # insertar después de iguales

        # 3) Construir nuevas listas insertadas
        # keys: insertamos en pos
        new_keys = keys[:pos] + [key] + keys[pos:]

        # pages[1:]: pares asociados a keys; insertamos page_pos en el mismo pos
        tail_pages = pages[1:]  # [p1, p2, ..., p_{m-1}]
        new_tail_pages = tail_pages[:pos] + [page_pos] + tail_pages[pos:]

        # Sanidad: tras insertar, deben tener misma longitud
        assert len(new_keys) == len(new_tail_pages)

        new_size = len(pages) + 1  # old_size = len(pages); sumamos 1 página nueva
        p0 = pages[0]

        # 4) Reescribir archivo completo
        with open(self.file_name, 'r+b') as file:
            file.seek(0)
            file.write(struct.pack(self.FORMAT_HEADER, new_size))  # size
            file.write(struct.pack('i', p0))  # p0
            for ki, pi in zip(new_keys, new_tail_pages):  # (k_i, p_i) para i>=1
                file.write(struct.pack('i', ki))
                file.write(struct.pack('i', pi))
            file.truncate(file.tell())

        return True

    def search_position(self, record_id: int):
        pages, keys = self.getIndex()

        if len(keys) == 0:
            return "START"

        left, right = 0, len(keys) - 1

        while left <= right:
            mid = (left + right) // 2
            if keys[mid] < record_id:
                left = mid + 1
            else:
                right = mid - 1

        if left == len(keys):
            return "END"

        return left + 1


    def scanALL(self):
        try:
            with open(self.file_name, 'rb') as file:
                size = struct.unpack(self.FORMAT_HEADER, file.read(self.SIZE_HEADER))[0]
                print("Index Size = ", str(size))
            pages, keys = self.getIndex()
            print("Pages: ", end='')
            for page in pages:
                print(str(page), end=", ")
            print()
            print("Keys: ", end='')
            for key in keys:
                print(str(key), end=", ")
        except FileNotFoundError:
            print("File not found")

class ISAM:
    def __init__(self, file_name):
        self.file_name = file_name

    def add(self, record: Record):
        indexf = IndexFile("index.dat")
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'wb') as file:
                new_page = Page([record])
                page_pos = file.tell()
                indexf.addIndex(page_pos, record.id)
                file.write(new_page.pack())
            return

        position = indexf.search_position(record.id)

        with open(self.file_name, 'r+b') as file:
            if position == "START" or position == "END":
                file.seek(0, 2)
                new_page = Page([record])
                page_pos = file.tell()
                indexf.addIndex(page_pos, record.id)
                file.write(new_page.pack())
                return "Added"
            else:
                pages, keys = indexf.getIndex()
                page_pos = pages[position - 1]
                file.seek(page_pos)
                page = Page.unpack(file.read(Page.SIZE_OF_PAGE))

                if len(page.records) + 1 > BLOCK_FACTOR:

                    record_position = page.position(record.id)

                    # crecemos en 1 para tener hueco
                    page.records.append(page.records[-1] if page.records else record)

                    # desplazamos a la derecha
                    for i in range(len(page.records) - 1, record_position, -1):
                        page.records[i] = page.records[i - 1]

                    # colocamos el nuevo
                    page.records[record_position] = record

                    r1 = page.records[:len(page.records)//2]
                    r2 = page.records[len(page.records)//2:]

                    page.records = r1

                    file.seek(page_pos)
                    file.write(page.pack())

                    file.seek(0, 2)
                    new_page = Page(r2)
                    page_pos = file.tell()
                    indexf.updateIndex(page_pos, r2[0].id)
                    file.write(new_page.pack())

                    return "Added new Page at the end"

                record_position = page.position(record.id)

                # crecemos en 1 para tener hueco
                page.records.append(page.records[-1] if page.records else record)

                # desplazamos a la derecha
                for i in range(len(page.records) - 1, record_position, -1):
                    page.records[i] = page.records[i - 1]

                # colocamos el nuevo
                page.records[record_position] = record

                file.seek(page_pos)
                file.write(page.pack())

                return "Record inserted successfully"

    def search(self, record_id: int):
        pass

    def delete(self, record_id: int):
        pass

    def scanAll(self):
        # Iterar en todas las paginas y mostrar la informacion de los registros
        with open(self.file_name, 'rb') as file:
            file.seek(0, 2)
            numPages = file.tell() // Page.SIZE_OF_PAGE
            file.seek(0, 0)
            print()
            for i in range(numPages):
                print("-- Page ", i + 1)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                for record in page.records:
                    print(record)

## Main
isamf = ISAM("data.dat")
indexf = IndexFile("index.dat")

records = []

with open("sales_dataset_unsorted.csv", newline='', encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile, delimiter=';')
    next(reader)  # saltamos el encabezado

    for row in reader:
        # row = [id, nombre, cantidad, precio, fecha]
        id_prod = int(row[0])
        nombre = row[1][:40]                 # ajustamos a 40 chars
        cantidad = int(row[2])
        precio = float(row[3])
        fecha = row[4][:15]                  # ajustamos a 15 chars

        record = Record(id_prod, nombre, cantidad, precio, fecha)
        records.append(record)

for record in records:
    isamf.add(record)

indexf.scanALL()
isamf.scanAll()

