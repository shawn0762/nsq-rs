pub enum Resp {
    Response = 0,
    Error = 1,
    Message = 2,
}

//          response frame
// [x][x][x][x][x][x][x][x][x][x][x][x]...
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  ||  N-byte
// ------------------------------------...
//     size     frame type     data
// size = 4 + N

impl Resp {
    pub fn new() {
        //
    }
}
