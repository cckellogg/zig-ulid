const std = @import("std");

pub usingnamespace @import("ulid.zig");

test {
    std.testing.refAllDecls(@This());
}
