package everyos.ggd.server.server;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public final class R1025Proto {

    private R1025Proto() {}

    public static final class Writer {

        private final ByteArrayOutputStream out =
            new ByteArrayOutputStream();

        public Writer varint(int field, long value) {

            writeVarint(
                ((long) field << 3) | 0
            );

            writeVarint(value);

            return this;
        }

        public Writer bool(int field, boolean value) {
            return varint(field, value ? 1 : 0);
        }

        public Writer string(int field, String value) {

            return bytes(
                field,
                value.getBytes(StandardCharsets.UTF_8)
            );
        }

        public Writer bytes(int field, byte[] value) {

            writeVarint(
                ((long) field << 3) | 2
            );

            writeVarint(value.length);

            out.writeBytes(value);

            return this;
        }

        public Writer message(int field, byte[] value) {
            return bytes(field, value);
        }

        public byte[] toByteArray() {
            return out.toByteArray();
        }

        private void writeVarint(long value) {

            while ((value & ~0x7FL) != 0) {

                out.write(
                    (int) ((value & 0x7F) | 0x80)
                );

                value >>>= 7;
            }

            out.write((int) value);
        }
    }

    public static final class Reader {

        private final byte[] data;
        private int position;

        public Reader(byte[] data) {
            this.data = data;
        }

        public boolean hasNext() {
            return position < data.length;
        }

        public long readVarint() {

            long result = 0;
            int shift = 0;

            while (true) {

                if (position >= data.length) {
                    throw new IllegalStateException(
                        "Unexpected end of protobuf"
                    );
                }

                int b = data[position++] & 0xff;

                result |=
                    (long) (b & 0x7f) << shift;

                if ((b & 0x80) == 0) {
                    return result;
                }

                shift += 7;

                if (shift > 63) {
                    throw new IllegalStateException(
                        "Invalid protobuf varint"
                    );
                }
            }
        }

        public byte[] readBytes() {

            int length = (int) readVarint();

            if (length < 0 ||
                position + length > data.length) {

                throw new IllegalStateException(
                    "Invalid protobuf length"
                );
            }

            byte[] result =
                new byte[length];

            System.arraycopy(
                data,
                position,
                result,
                0,
                length
            );

            position += length;

            return result;
        }

        public String readString() {

            return new String(
                readBytes(),
                StandardCharsets.UTF_8
            );
        }

        public void skip(int wire) {

            switch (wire) {

                case 0:
                    readVarint();
                    break;

                case 1:
                    position += 8;
                    break;

                case 2:
                    int length = (int) readVarint();
                    position += length;
                    break;

                case 5:
                    position += 4;
                    break;

                default:
                    throw new IllegalStateException(
                        "Unsupported protobuf wire type: "
                            + wire
                    );
            }

            if (position > data.length) {
                throw new IllegalStateException(
                    "Skipped past protobuf packet"
                );
            }
        }
    }
}