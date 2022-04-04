#![feature(let_else)]
use tracy_client;

/*
2017-04-01 12:39:58.932 UTC,YOR7S+IR7NbK39XxMBjZTw==,994,995,15
2017-04-01 08:37:11.451 UTC,LWiku7cCJVE4Ga2rzNKkaw==,821,899,15
2017-04-01 17:51:08.568 UTC,P6lg1nALc3PHWc3rzDi19Q==,971,969,15
2017-04-01 00:41:05.975 UTC,J6JO3thYinHc4d/pA3SpDg==,621,461,15
*/

use core::panic;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
struct Placement {
    timestamp: u64,
    x: u16,
    y: u16,
    color: u8,
}

fn parse_csv() -> Vec<Placement> {
    use std::io::BufRead;

    let file_path = std::fs::File::open("./data/place_tiles").unwrap();

    //this will stream the lines
    //seems a bit slow, we need to be fast to prepare for 2022 r/place data

    let buf_size = 1024 * 1024; //1mb
    let mut lines = std::io::BufReader::with_capacity(buf_size, file_path).lines();
    lines.next(); //skip header

    let mut placements = vec![];

    eprintln!("Parsing all data...");
    let now = std::time::Instant::now();

    for line in lines {
        let Ok(csv_line) = line else {
            panic!("Error getting line!");
        };

        let mut split: std::str::Split<_> = csv_line.split(",");

        let timestamp_str = split.next().unwrap();

        let date =
            chrono::NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S%.f UTC")
                .unwrap();
        let timestamp = date.timestamp_millis() as u64;
        split.next(); //we don't care who set the pixel

        let x_str = split.next().unwrap();
        if x_str == "" {
            continue;
        }

        let x: u16 = x_str.parse().unwrap();
        let y: u16 = split.next().unwrap().parse().unwrap();
        let color: u8 = split.next().unwrap().parse().unwrap();

        placements.push(Placement {
            timestamp,
            x,
            y,
            color,
        });
    }

    eprintln!("Done parsing! took {}ms", now.elapsed().as_millis());
    return placements;
}

fn sort_csv(mut placements: Vec<Placement>) -> Vec<Placement> {
    eprintln!("Sorting all data...");
    let now = std::time::Instant::now();

    placements.sort_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap());

    eprintln!("Done sorting! took {}ms", now.elapsed().as_millis());

    return placements;
}

fn write_placements_as_binary(placements: &[Placement], file_path: &str) {
    //we have it parsed, now we can just write to disk in binary format. Hopefully it will be much faster to load later
    let mut open_options = OpenOptions::new();
    open_options.write(true).truncate(true).create(true);

    let mut writer = std::io::BufWriter::new(open_options.open(file_path).unwrap());
    //first: write the length
    writer
        .write(&(placements.len() as u64).to_le_bytes())
        .unwrap();

    for placement in placements {
        //then we write all the data, no separator
        writer.write(&placement.timestamp.to_le_bytes()).unwrap();
        writer.write(&placement.x.to_le_bytes()).unwrap();
        writer.write(&placement.y.to_le_bytes()).unwrap();
        writer.write(&placement.color.to_le_bytes()).unwrap();
    }

    writer.flush().unwrap();
}

macro_rules! from_bytes_primitive {
    ($type:ty, $reader:expr) => {
        {
            const BYTE_LEN: usize = std::mem::size_of::<$type>();
            let mut buf = [0u8; BYTE_LEN];
            $reader.read_exact(&mut buf).unwrap();//(&format!("Tried to read {} bytes but failed", BYTE_LEN));
            <$type>::from_le_bytes(buf)
        }
    };
}

fn read_placements(file_path: &str) -> Vec<Placement> {
    tracy_client::start_noncontinuous_frame!("read_parsed");

    let mut writer =
        std::io::BufReader::with_capacity(1024 * 1024, std::fs::File::open(file_path).unwrap());

    eprintln!("Reading all data...");
    let now = std::time::Instant::now();

    let length: u64 = from_bytes_primitive!(u64, &mut writer);
    let mut placement = vec![];
    placement.reserve(length as usize);

    eprintln!("Will read {} placements", length);
    for _ in 0..length {
        let timestamp = from_bytes_primitive!(u64, &mut writer);
        let x = from_bytes_primitive!(u16, &mut writer);
        let y = from_bytes_primitive!(u16, &mut writer);
        let color = from_bytes_primitive!(u8, &mut writer);
        placement.push(Placement {
            timestamp,
            x,
            y,
            color,
        })
    }
    eprintln!(
        "Done reading all data! took {}ms",
        now.elapsed().as_millis()
    );
    return placement;
}

fn parse_sort_and_save() {
    let placements = parse_csv();
    for placement in placements.iter().take(10) {
        eprintln!("{:?}", placement);
    }
    let sorted = sort_csv(placements);
    for placement in sorted.iter().take(10) {
        eprintln!("{:?}", placement);
    }
    unsafe { write_placements_as_binary_unsafe(&sorted, "./data/parsed_unsafe_write") }
}

//We have ~1GB worth of csv, and we're taking ~7 seconds to parse and sort the CSV
//This gives a throughput of 142MB/s which is not bad

//However, after we record the parsed and sorted data to a binary file, reading it is taking ~3 seconds,
//and the file is 200MB, which gives us a throughput of 66MB/s. Not good enough.

//Profiling on tracy showed a call to format, removing the call now results in 125ms instead of 3 seconds. Pretty good :) but that's like 1.5GB/s
//After restarting the computer, parsing took 926ms, which gives us 215MB/s, which seems much more reasonable instead of 1.5GB/s

//let's try to make the reading a bit different :) Instead of writing field by field, let's write the entire array at once
//and read it at once too, and then cast the raw pointer to the array we want. **Highly unsafe** but should be fun.

//writes extra data, perhaps due to alignment
unsafe fn write_placements_as_binary_unsafe(placements: &[Placement], file_path: &str) {
    let as_u8 = std::slice::from_raw_parts(
        (&placements[0] as *const Placement) as *const u8,
        ::std::mem::size_of::<Placement>() * placements.len(),
    );

    let mut open_options = OpenOptions::new();
    open_options.write(true).truncate(true).create(true);

    let mut file = open_options.open(file_path).unwrap();
    file.write_all(&(placements.len() as u64).to_le_bytes())
        .unwrap();
    file.write_all(as_u8).unwrap();
    file.flush().unwrap();
}

unsafe fn read_placements_unsafe(file_path: &str) -> &[Placement] {
    eprintln!("Reading all data...");
    let now = std::time::Instant::now();

    let mut buf = vec![];
    std::fs::File::open(file_path)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    //let's leak the contents of the vec
    let leaked = buf.leak();

    let placement_len = *((&leaked[0] as *const u8) as *const u64);
    eprintln!("Placements: {}", placement_len);
    let after_len = ((&leaked[0] as *const u8) as *const u64).offset(1) as *const Placement;
    let placements = std::slice::from_raw_parts(after_len, placement_len as usize);

    eprintln!(
        "Done reading all data! took {}ms",
        now.elapsed().as_millis()
    );
    return placements;
}

/*
fn main() {
    let placements = read_placements("./data/parsed3");

    let fields_size_bytes = (64 + 32 + 8) / 8;
    let struct_size_bytes = std::mem::size_of::<Placement>();
    dbg!(fields_size_bytes);
    dbg!(struct_size_bytes);

    eprintln!("Expected file size with safe write: {} bytes", placements.len() * fields_size_bytes);
    eprintln!("Expected file size with unsafe write: {} bytes", placements.len() * struct_size_bytes);


    parse_sort_and_save();
    for placement in placements.iter().take(10) {
        eprintln!("{:?}", placement);
    }
}
*/

fn main() {
    use image::{ImageBuffer, RgbImage};
    let placements = read_placements("./data/parsed3");
    //let's build the image
    let mut img: RgbImage = ImageBuffer::new(1024, 1024);
    for x in 0..1000 {
        for y in 0..1000 {
            let pixel = img.get_pixel_mut(x, y);
            *pixel = image::Rgb([255, 255, 255]);
        }
    }

    let fps = 60;
    let duration_seconds = 60;
    let total_frames = duration_seconds * fps;
    let skip_and_record = placements.len() / total_frames;

    //std::fs::create_dir("output");
    let mut bytes: Vec<u8> = Vec::new();

    let mut frame = 0;
    for (placement_idx, placement) in placements.iter().enumerate() {
        if placement.x >= 1000 || placement.y >= 1000 {
            continue;
        }
        let pixel = img.get_pixel_mut(placement.x as u32, placement.y as u32);
        let color: image::Rgb<u8> = match placement.color {
            0 => image::Rgb([0xFF, 0xFF, 0xFF]),
            1 => image::Rgb([0xE4, 0xE4, 0xE4]),
            2 => image::Rgb([0x88, 0x88, 0x88]),
            3 => image::Rgb([0x22, 0x22, 0x22]),
            4 => image::Rgb([0xFF, 0xA7, 0xD1]),
            5 => image::Rgb([0xE5, 0x00, 0x00]),
            6 => image::Rgb([0xE5, 0x95, 0x00]),
            7 => image::Rgb([0xA0, 0x6A, 0x42]),
            8 => image::Rgb([0xE5, 0xD9, 0x00]),
            9 => image::Rgb([0x94, 0xE0, 0x44]),
            10 => image::Rgb([0x02, 0xBE, 0x01]),
            11 => image::Rgb([0x00, 0xE5, 0xF0]),
            12 => image::Rgb([0x00, 0x83, 0xC7]),
            13 => image::Rgb([0x00, 0x00, 0xEA]),
            14 => image::Rgb([0xE0, 0x4A, 0xFF]),
            15 => image::Rgb([0x82, 0x00, 0x80]),
            _ => panic!("Unknown color index"),
        };
        *pixel = color;

        if placement_idx % skip_and_record == 0 || placement_idx == placements.len() - 1 {
            //eprintln!("Recording frame {}", frame);
            img.write_to(&mut std::io::Cursor::new(&mut bytes), image::ImageOutputFormat::Bmp).unwrap();
            std::io::stdout().write(&bytes).unwrap();
            frame+=1;
        }
    }

    
    
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn writes_binary_and_reads_back() {
        let placements = vec![
            Placement {
                //trying to fill 8 bytes with a known pattern
                timestamp: 0xfffffffffffffffd,
                x: 0,
                y: 1,
                color: 2,
            },
            Placement {
                timestamp: 0xfffffffffffffffe,
                x: 5,
                y: 6,
                color: 3,
            },
            Placement {
                timestamp: 0xffffffffffffffff,
                x: 5,
                y: 6,
                color: 3,
            },
        ];
        write_placements_as_binary(&placements, "./data/unit_test_parsed");
        let read_data = read_placements("./data/unit_test_parsed");

        assert_eq!(placements, read_data);
    }
}
