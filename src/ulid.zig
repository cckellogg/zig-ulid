const std = @import("std");
const builtin = @import("builtin");

const time = std.time;
const print = std.debug.print;
const testing = std.testing;
const crypto_random = std.crypto.random;

const Error = error{
    InvalidLength,
    InvalidChar,
    InvalidTimestamp,
    InvalidRandom,
    MonotonicOverflowError,
};

threadlocal var previous_ulid: Ulid = Ulid{
    .bits = 0,
};

/// base32 encoding characters
/// https://www.crockford.com/base32.html
///
const encoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// This stores a mapping of utf-8 characters to
/// the index into the base32 encoding characters.
/// If the value is 'nval' then we are not decoding
/// a ulid string.
const nval: u8 = 255;
const decoding = [_]u8{
    // 0 - 9
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 10 - 19
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 20 - 29
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 30 - 39
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 40 - 49
    nval, nval, nval, nval, nval, nval, nval, nval, 0,    1,
    // 50 - 59
    2,    3,    4,    5,    6,    7,    8,    9,    nval, nval,
    // 60 - 69
    nval, nval, nval, nval, nval, 10,   11,   12,   13,   14,
    // 70 - 79
    15,   16,   17,   1,    18,   19,   1,    20,   21,   0,
    // 80 - 89
    22,   23,   24,   25,   26,   nval, 27,   28,   29,   30,
    // 90 - 99
    31,   nval, nval, nval, nval, nval, nval, 10,   11,   12,
    // 100 - 101
    13,   14,   15,   16,   17,   1,    18,   19,   1,    20,
    // 110 - 119
    21,   0,    22,   23,   24,   25,   26,   nval, 27,   28,
    // 120 - 129
    29,   30,   31,   nval, nval, nval, nval, nval, nval, nval,
    // 130 - 139
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 140 - 149
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 150 - 159
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 160 - 169
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 170 - 179
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 180 - 189
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 190 - 199
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 200 - 209
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 210 - 219
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 220 - 229
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 230 - 239
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 240 - 249
    nval, nval, nval, nval, nval, nval, nval, nval, nval, nval,
    // 250 - 255
    nval, nval, nval, nval, nval, nval,
};

/// Max epoch time in milliseconds
pub const max_timestamp: u64 = 281474976710655;

/// Max random number
pub const max_random: u128 = (1 << 80) - 1;

/// Number of characters in an Ulid string
const ulid_string_len = 26;

/// Universally Unique Lexicographically Sortable Identifier
/// https://github.com/ulid/spec
///
///  01AN4Z07BY      79KA1307SR9X4MV3
/// |----------|    |----------------|
///   Timestamp          Randomness
///    48bits             80bits
///
pub const Ulid = struct {
    bits: u128,

    /// Encode the bits as a base32 string
    pub fn encode(self: Ulid, dest: []u8) ![]u8 {
        return encodeBase32(self.bits, dest);
    }

    pub fn binary(self: Ulid) u128 {
        return self.bits;
    }

    /// Return the timestamp bits of the ulid
    pub fn timestamp(self: Ulid) u64 {
        return @intCast((self.bits >> 80));
    }

    /// Return the random bits of the ulid
    pub fn random(self: Ulid) u128 {
        return self.bits & max_random;
    }
};

/// Returns a random u128 `i` such that `0 <= i <= max_random`.
fn randomInt() u128 {
    const rand_upper_bits = (@as(u128, crypto_random.int(u64)) << 16);
    const rand_lower_bits = @as(u128, crypto_random.int(u16));
    return (rand_upper_bits | rand_lower_bits);
}

/// Return a ulid with the provided timestamp
pub fn fromTimestamp(timestamp: u64) !Ulid {
    return fromParts(timestamp, randomInt());
}

/// Return a ulid from the provided timestamp and random number.
pub fn fromParts(timestamp: u64, random: u128) !Ulid {
    if (timestamp > max_timestamp) {
        return Error.InvalidTimestamp;
    }
    if (random > max_random) {
        return Error.InvalidRandom;
    }
    var ts: u128 = timestamp;
    return Ulid{
        .bits = (ts << 80) | random,
    };
}

/// Encode the bits using base32 into the provided slice using
fn encodeBase32(bits: u128, dest: []u8) ![]u8 {
    const out_len = ulid_string_len;
    if (dest.len < out_len) {
        return Error.InvalidLength;
    }

    var remaining = bits;
    var index: u8 = 0;
    const last_index = out_len - 1;
    const mask = @as(u128, 31);
    while (index < out_len) {
        const encoding_idx: u8 = @intCast((remaining & mask));
        dest[last_index - index] = encoding[encoding_idx];
        remaining >>= 5;
        index += 1;
    }

    return dest[0..out_len];
}

/// Parse the base32 string into a Ulid
fn parse(str: []const u8) !Ulid {
    if (str.len != ulid_string_len) {
        return Error.InvalidLength;
    }
    var bits: u128 = 0;
    for (str) |c| {
        const encoding_index = decoding[@as(u8, c)];
        if (nval == encoding_index) {
            return Error.InvalidChar;
        }
        bits = (bits << 5) | @as(u128, encoding_index);
    }

    const ulid = Ulid{
        .bits = bits,
    };

    if (ulid.timestamp() > max_timestamp) {
        return Error.InvalidTimestamp;
    }

    return ulid;
}

/// Returns a new Ulid. If the previously generated
/// Ulid is within the same millisecond the random component
/// is incremented by 1 bit. If the there is an overflow
/// `MonotonicOverflowError` is returned.
pub fn generate() !Ulid {
    const now = std.math.absCast(time.milliTimestamp());
    if (now == previous_ulid.timestamp()) {
        const random = previous_ulid.random();
        if (max_random == random) {
            return Error.MonotonicOverflowError;
        }
        const next = try fromParts(now, random + 1);
        previous_ulid.bits = next.bits;
        return next;
    } else {
        const next = try fromTimestamp(now);
        previous_ulid.bits = next.bits;
        return next;
    }
}

test "encoding" {
    const numbers = "0123456789";
    for (numbers) |n| {
        const index: u8 = @as(u8, n);
        const encoding_index = decoding[index];
        const encoded_char = encoding[encoding_index];
        try testing.expectEqual(n, encoded_char);
    }

    const chars_to_0 = "Oo";
    for (chars_to_0) |c| {
        const index: u8 = @as(u8, c);
        const encoding_index = decoding[index];
        try testing.expectEqual(@as(u8, 0), encoding_index);
    }

    const chars_to_1 = "IiLl";
    for (chars_to_1) |c| {
        const index: u8 = @as(u8, c);
        const encoding_index = decoding[index];
        try testing.expectEqual(@as(u8, 1), encoding_index);
    }

    const chars = @as([]const u8, encoding)[10..];
    for (chars) |c| {
        const upper_index = decoding[@as(u8, c)];
        try testing.expectEqual(c, encoding[upper_index]);

        const lower = std.ascii.toLower(c);
        const lower_index = decoding[@as(u8, lower)];
        try testing.expectEqual(c, encoding[lower_index]);
    }
}

test "encoding invalid characters" {
    const invalid_chars = "Uu@";
    for (invalid_chars) |c| {
        const index: u8 = @as(u8, c);
        const encoding_index = decoding[index];
        try testing.expectEqual(nval, encoding_index);
    }
}

test "ulid.timestamp" {
    const ts = @as(u128, 1469918176385);
    var ulid = Ulid{ .bits = (ts << 80) };
    try testing.expectEqual(@as(u64, 1469918176385), ulid.timestamp());
}

test "ulid.random" {
    const randbits = randomInt();
    var ulid = try fromParts(0, randbits);
    try testing.expectEqual(randbits, ulid.random());
}

test "encodeBase32" {
    const ts = @as(u128, 1469918176385);
    const ulid_bits: u128 = (ts << 80);

    var buf: [ulid_string_len]u8 = undefined;
    const ulid_str = try encodeBase32(ulid_bits, &buf);
    const expected = "01ARYZ6S410000000000000000";
    try testing.expect(std.mem.eql(u8, expected, ulid_str));

    const max_ts = @as(u128, 281474976710655);
    const max_ts_ulid_bits = (max_ts << 80);
    const max_ts_ulid = try encodeBase32(max_ts_ulid_bits, &buf);

    const expected_max_ts_ulid = "7ZZZZZZZZZ0000000000000000";
    try testing.expect(std.mem.eql(u8, expected_max_ts_ulid, max_ts_ulid));

    const max_ulid = try fromParts(max_timestamp, max_random);
    const max_ulid_str = try encodeBase32(max_ulid.bits, &buf);
    const expected_max_ulid = "7ZZZZZZZZZZZZZZZZZZZZZZZZZ";
    try testing.expect(std.mem.eql(u8, expected_max_ulid, max_ulid_str));

    // test buffer too small
    var buf_too_small: [ulid_string_len - 1]u8 = undefined;
    try testing.expectError(error.InvalidLength, encodeBase32(0, &buf_too_small));
}

test "fromTimestamp" {
    const ts = @as(u64, 1469918176385);
    const ulid = try fromTimestamp(ts);
    try testing.expectEqual(ts, ulid.timestamp());

    // test max timestamp
    const max_ts_ulid = try fromTimestamp(max_timestamp);
    try testing.expectEqual(max_timestamp, max_ts_ulid.timestamp());

    // test over max timestamp
    try testing.expectError(error.InvalidTimestamp, fromTimestamp(max_timestamp + 1));
}

test "fromParts" {
    const ts = @as(u64, 1469918176385);
    const ulid = try fromTimestamp(ts);
    try testing.expectEqual(ts, ulid.timestamp());

    // test max timestamp
    const max_ts_ulid = try fromTimestamp(max_timestamp);
    try testing.expectEqual(max_timestamp, max_ts_ulid.timestamp());

    // test over max timestamp
    try testing.expectError(error.InvalidTimestamp, fromTimestamp(max_timestamp + 1));
}

test "parse" {
    const valid = struct {
        str: []const u8,
        ts: u64,
        rand: u128,
    };
    const valid_cases = [_]valid{
        valid{
            .str = @as([]const u8, "01ARYZ6S410000000000000000"),
            .ts = 1469918176385,
            .rand = 0,
        },
        valid{
            .str = @as([]const u8, "7ZZZZZZZZZ0000000000000000"),
            .ts = max_timestamp,
            .rand = 0,
        },
        valid{
            .str = @as([]const u8, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"),
            .ts = max_timestamp,
            .rand = max_random,
        },
    };

    for (valid_cases) |t| {
        const ulid = try parse(t.str);
        try testing.expectEqual(t.ts, ulid.timestamp());
        try testing.expectEqual(t.rand, ulid.random());
    }

    const invalid = struct {
        str: []const u8,
        err: Error,
    };
    const invalid_cases = [_]invalid{
        invalid{
            .str = @as([]const u8, "01ARYZ6S4100000000000000000"), // too long
            .err = Error.InvalidLength,
        },
        invalid{
            .str = @as([]const u8, "01ARYZ6S41000000000000000"), // too short
            .err = Error.InvalidLength,
        },
        invalid{
            .str = @as([]const u8, "@1ARYZ6S410000000000000000"),
            .err = Error.InvalidChar,
        },
    };

    for (invalid_cases) |t| {
        try testing.expectError(t.err, parse(t.str));
    }
}

test "threadlocal.previous_ulid" {
    // This test requires spawning threads.
    // https://github.com/ziglang/zig/issues/1908
    if (builtin.single_threaded) return error.SkipZigTest;

    const Runner = struct {
        mutex: std.Thread.Mutex = .{},
        condition: std.Thread.Condition = .{},
        finished: bool = false,

        fn gen(self: *@This()) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            defer self.condition.signal();
            self.finished = true;
            const ulid = try generate();
            try testing.expect(@as(u128, 0) < previous_ulid.bits);
            try testing.expectEqual(ulid.bits, previous_ulid.bits);
        }

        fn noGen(self: *@This()) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            // wait for gen to finish
            while (!self.finished) {
                self.condition.wait(&self.mutex);
            }
            try testing.expectEqual(@as(u128, 0), previous_ulid.bits);
        }
    };

    var runner = Runner{};
    const thread1 = try std.Thread.spawn(.{}, Runner.gen, .{&runner});
    const thread2 = try std.Thread.spawn(.{}, Runner.noGen, .{&runner});
    thread1.join();
    thread2.join();
}
