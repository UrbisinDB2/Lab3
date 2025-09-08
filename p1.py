import struct, os
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
BLOCK_FACTOR = 5
INDEX_FACTOR = 5

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

class IndexFile:
    FORMAT_HEADER = 'i'
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_INDEX = SIZE_HEADER + INDEX_FACTOR

    def __init__(self, file_name):
        self.file_name = file_name

    def search_position(self, record_id: int):
        record_size = struct.calcsize('ii')  # min_key, page_no

        with open(self.file_name, 'rb') as file:
            file.seek(0, 2)
            num_entries = file.tell() // record_size
            left, right = 0, num_entries - 1
            result_page = None

            while left <= right:
                mid = (left + right) // 2
                file.seek(mid * record_size, 0)
                data = file.read(record_size)
                min_key, page_no = struct.unpack('ii', data)

                if record_id >= min_key:
                    result_page = page_no
                    left = mid + 1  # buscar a la derecha
                else:
                    right = mid - 1  # buscar a la izquierda

            # Si no encontró nada, por defecto va a la primera página
            return result_page if result_page is not None else 0

    def build(self, data_file: str):
        with open(data_file, 'rb') as df, open(self.file_name, 'wb') as idxf:
            df.seek(0, 2)
            numPages = df.tell() // Page.SIZE_OF_PAGE
            df.seek(0, 0)
            for page_num in range(numPages):
                page_data = df.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                if page.records:
                    first_key = page.records[0].id
                    idxf.write(struct.pack(self.FORMAT, first_key, page_num))

    def scanALL(self):
        try:
            with open(self.file_name, 'rb') as file:
                while entry := file.read(self.SIZE_OF_RECORD):
                    key, page_pos = struct.unpack(self.FORMAT, entry)
                    print(f"Key: {key} | Page: {page_pos}")
        except FileNotFoundError:
            print("File not found")

class ISAM:
    def __init__(self, file_name):
        self.file_name = file_name

    def add(self, record: Record):
        # 1- si el archivo no existe, crearlo con una sola pagina y registro
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'wb') as file:
                new_page = Page([record])
                file.write(new_page.pack())
            return
        # 2- si el archivo existe, recuperar la ultima pagina
        # 2.1- agregar el registro y regresar la pagina al archivo
        indexf = IndexFile("index.dat")
        indexf._search_position(record.id)

        with open(self.file_name, 'r+b') as file:
            file.seek(0, 2)
            filesize = file.tell()
            pos_last_page = filesize - Page.SIZE_OF_PAGE
            file.seek(pos_last_page, 0)
            page = Page.unpack(file.read(Page.SIZE_OF_PAGE))
            # si hay espacio, agregamos el registro a la pagina
            if len(page.records) < BLOCK_FACTOR:
                page.records.append(record)
                file.seek(pos_last_page, 0)
                file.write(page.pack())
            else:
                # crear nueva pagina
                file.seek(0, 2)
                new_page = Page([record])
                file.write(new_page.pack())

    def scanAll(self):
        # Iterar en todas las paginas y mostrar la informacion de los registros
        with open(self.file_name, 'rb') as file:
            file.seek(0, 2)
            numPages = file.tell() // Page.SIZE_OF_PAGE
            file.seek(0, 0)
            for i in range(numPages):
                print("-- Page ", i + 1)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                for record in page.records:
                    print(record)

## Main
indexf = IndexFile("index.dat")
dataf = DataFile("data.dat")
dataf.add(Record(1, "Estabilizador de Voltaje", 25, 192.26, "2024-10-21"))
dataf.add(Record(2, "Bascula Inteligente", 43, 1809.71, "2024-05-07"))
dataf.add(Record(3, "Estabilizador de Voltaje", 7, 1204.21, "2024-08-21"))
indexf.scanALL()
dataf.scanAll()

