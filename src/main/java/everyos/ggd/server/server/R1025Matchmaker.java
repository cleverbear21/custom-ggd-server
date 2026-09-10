package everyos.ggd.server.server.r1025;

import java.util.UUID;

import everyos.ggd.server.server.R1025Proto;

public final class R1025Matchmaker {

    private R1025Matchmaker() {}

    public static boolean isHalloweenRequest(byte[] payload) {

        R1025Proto.Reader outer =
            new R1025Proto.Reader(payload);

        while (outer.hasNext()) {

            long tag = outer.readVarint();

            int field = (int) (tag >>> 3);
            int wire = (int) (tag & 7);

            if (field != 1 || wire != 2) {
                outer.skip(wire);
                continue;
            }

            byte[] gx = outer.readBytes();

            R1025Proto.Reader request =
                new R1025Proto.Reader(gx);

            while (request.hasNext()) {

                long gxTag =
                    request.readVarint();

                int gxField =
                    (int) (gxTag >>> 3);

                int gxWire =
                    (int) (gxTag & 7);

                if (gxField != 1 || gxWire != 2) {
                    request.skip(gxWire);
                    continue;
                }

                String value =
                    request.readString();

                if (
                    "GAME_MODE:HALLOWEEN".equals(value)
                    ||
                    "GAME_MODE:HALLOWEEN_PRIVATE".equals(value)
                ) {
                    return true;
                }
            }
        }

        return false;
    }

    public static byte[] createSuccessResponse() {

        int de = 1;

        String fc =
            UUID.randomUUID().toString();

        byte[] tx =
            new R1025Proto.Writer()
                .varint(1, de)
                .string(2, fc)
                .toByteArray();

        byte[] ux =
            new R1025Proto.Writer()
                .varint(1, 1)
                .string(2, "gs.local.cloud.doodles.goog")
                .message(4, tx)
                .toByteArray();

        return new R1025Proto.Writer()
            .message(1, ux)
            .toByteArray();
    }

    public static byte[] wrapApplication(
        byte[] application
    ) {

        byte[] envelope =
            new R1025Proto.Writer()
                .string(1, "/m")
                .bytes(2, application)
                .toByteArray();

        return new R1025Proto.Writer()
            .varint(1, 3)
            .message(2, envelope)
            .toByteArray();
    }
}