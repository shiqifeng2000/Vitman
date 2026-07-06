use std::ffi::CString;

use anyhow::Result;

#[derive(Debug)]
pub struct VadConfig {
    pub(crate) cfg: sherpa_sys::SherpaOnnxVadModelConfig,
}

#[derive(Debug)]
pub struct Vad {
    pub(crate) vad: *mut sherpa_sys::SherpaOnnxVoiceActivityDetector,
}

impl VadConfig {
    pub fn new(
        model: String,
        min_silence_duration: f32,
        min_speech_duration: f32,
        threshold: f32,
        sample_rate: i32,
        window_size: i32,
        provider: Option<String>,
        num_threads: Option<i32>,
        debug: Option<bool>,
    ) -> Self {
        let provider = provider.unwrap_or(get_default_provider());
        let provider = CString::new(provider).unwrap();
        let model = CString::new(model).unwrap();

        let silero_vad = sherpa_sys::SherpaOnnxSileroVadModelConfig {
            model: model.into_raw(),
            min_silence_duration,
            min_speech_duration,
            threshold,
            window_size,
        };
        let debug = debug.unwrap_or(false);
        let debug = if debug { 1 } else { 0 };
        let cfg = sherpa_sys::SherpaOnnxVadModelConfig {
            debug,
            provider: provider.into_raw(),
            num_threads: num_threads.unwrap_or(1),
            sample_rate,
            silero_vad,
        };
        Self { cfg }
    }

    pub fn as_ptr(&self) -> *const sherpa_sys::SherpaOnnxVadModelConfig {
        &self.cfg
    }
}

#[derive(Debug)]
pub struct SpeechSegment {
    pub start: i32,
    pub samples: Vec<f32>,
}

impl Vad {
    pub fn new_from_config(config: VadConfig, buffer_size_in_seconds: f32) -> Result<Self> {
        unsafe {
            let vad = sherpa_sys::SherpaOnnxCreateVoiceActivityDetector(
                config.as_ptr(),
                buffer_size_in_seconds,
            );
            Ok(Self {
                vad: vad as *mut sherpa_sys::SherpaOnnxVoiceActivityDetector,
            })
        }
    }

    pub fn is_empty(&mut self) -> bool {
        unsafe { sherpa_sys::SherpaOnnxVoiceActivityDetectorEmpty(self.vad) == 1 }
    }

    pub fn front(&mut self) -> SpeechSegment {
        unsafe {
            let segment_ptr = sherpa_sys::SherpaOnnxVoiceActivityDetectorFront(self.vad);
            let raw_segment = segment_ptr.read();
            let samples: &[f32] =
                std::slice::from_raw_parts(raw_segment.samples, raw_segment.n as usize);

            let segment = SpeechSegment {
                samples: samples.to_vec(),
                start: raw_segment.start,
            };

            // Free
            sherpa_sys::SherpaOnnxDestroySpeechSegment(segment_ptr);

            segment
        }
    }

    pub fn flush(&mut self) {
        unsafe {
            sherpa_sys::SherpaOnnxVoiceActivityDetectorFlush(self.vad);
        }
    }

    pub fn accept_waveform(&mut self, mut samples: Vec<f32>) {
        let samples_ptr = samples.as_mut_ptr();
        let samples_length = samples.len();
        unsafe {
            sherpa_sys::SherpaOnnxVoiceActivityDetectorAcceptWaveform(
                self.vad,
                samples_ptr,
                samples_length.try_into().unwrap(),
            );
        };
    }

    pub fn pop(&mut self) {
        unsafe {
            sherpa_sys::SherpaOnnxVoiceActivityDetectorPop(self.vad);
        }
    }

    pub fn is_speech(&mut self) -> bool {
        unsafe { sherpa_sys::SherpaOnnxVoiceActivityDetectorDetected(self.vad) == 1 }
    }

    pub fn clear(&mut self) {
        unsafe {
            sherpa_sys::SherpaOnnxVoiceActivityDetectorClear(self.vad);
        }
    }
    pub fn reset(&mut self) {
        unsafe {
            sherpa_sys::SherpaOnnxVoiceActivityDetectorReset(self.vad);
        }
    }
}

unsafe impl Send for Vad {}
unsafe impl Sync for Vad {}

impl Drop for Vad {
    fn drop(&mut self) {
        unsafe {
            sherpa_sys::SherpaOnnxDestroyVoiceActivityDetector(self.vad);
        }
    }
}

pub fn get_default_provider() -> String {
    if cfg!(feature = "cuda") {
        "cuda"
    } else if cfg!(target_os = "macos") {
        "coreml"
    } else if cfg!(feature = "directml") {
        "directml"
    } else {
        "cpu"
    }
    .into()
}

#[test]
fn test_vad() {
    // use crate::utils;
    use std::io::Write;
    let _ = std::fs::remove_file("./output32.pcm");
    let _ = std::fs::remove_file("./output73.pcm");
    let _ = std::fs::remove_file("./output115.pcm");
    let vad_config = VadConfig::new(
        "/data/workspace/boe/DigtalTalk/models/silero_vad.onnx".to_owned(),
        0.5,
        0.5,
        0.5,
        16000,
        512,
        Some("cpu".to_owned()),
        None,
        None,
    );
    let mut vad = Vad::new_from_config(vad_config, 60f32).unwrap();

    // let wavfile = utils::str_to_c_str("/data/workspace/boe/rkdatabus/asset/sherpa_onnx/test.wav");
    // let mut inp_file = std::fs::File::open(std::path::Path::new(
    //     "/data/workspace/boe/DigtalTalk/models/test.wav",
    // ))
    // .unwrap();

    // let spec = hound::WavSpec {
    //     channels: 1,
    //     sample_rate: 16000,
    //     bits_per_sample: 16,
    //     sample_format: hound::SampleFormat::Int,
    // };
    let mut reader =
        hound::WavReader::open("/data/workspace/boe/rkdatabus/asset/sherpa_onnx/test.wav").unwrap();
    let mut samples = reader.samples::<i16>();
    let mut cache = vec![];
    let mut cursor = 0u32;
    // let mut f = std::fs::File::create("./test.pcm").unwrap();
    while let Some(Ok(sample)) = samples.next() {
        cache.push(sample as f32 / 0x8000 as f32);
        if cache.len() >= 2730 {
            vad.accept_waveform(cache.clone());
            // f.write_all(bytemuck::cast_slice::<f32, u8>(&cache));

            println!(
                "{} cursor {cursor} {} cache {}",
                vad.is_empty(),
                vad.is_speech(),
                cache.len()
            );
            if !vad.is_empty() {
                let mut file_out = std::fs::File::options()
                    .create(true)
                    .append(true)
                    .open(format!("output{cursor}.pcm"))
                    .unwrap();
                let mut cache1 = vec![];
                while !vad.is_empty() {
                    let front = vad.front();
                    println!("front samples {}", front.samples.len());
                    cache1.extend_from_slice(&front.samples);
                    vad.pop();
                    // vad.reset();
                }
                // vad.reset();
                let _ = file_out.write_all(bytemuck::cast_slice::<f32, u8>(&cache1));
            } else {
                cursor = cursor.wrapping_add(1u32);
            }

            cache.clear();
        }
    }
    // 关键步骤
    vad.flush();
    // vad.accept_waveform(cache);

    let mut file_out = std::fs::File::options()
        .create(true)
        .append(true)
        .open(format!("output{cursor}.pcm"))
        .unwrap();
    let mut cache1 = vec![];
    loop {
        let front = vad.front();
        if front.samples.len() == 0 {
            break;
        }
        cache1.extend_from_slice(&front.samples);
        vad.pop();
    }
    // while !vad.is_empty() {
    //     let front = vad.front();
    //     cache1.extend_from_slice(&front.samples);
    //     vad.pop();
    // }
    let _ = file_out.write_all(bytemuck::cast_slice::<f32, u8>(&cache1));
    // f.write_all(bytemuck::cast_slice::<f32, u8>(&cache));
    // vad.accept_waveform(cache.clone());

    // if !vad.is_empty() {
    //     let mut file_out = std::fs::File::options()
    //         .create(true)
    //         .append(true)
    //         .open(format!("output{cursor}.pcm"))
    //         .unwrap();
    //     let mut cache = vec![];
    //     while !vad.is_empty() {
    //         let front = vad.front();
    //         cache.extend_from_slice(&front.samples);
    //         vad.pop();
    //     }
    //     file_out.write_all(bytemuck::cast_slice::<f32, u8>(&cache));
    // }

    // cache.clear();
    // println!("RMS is {}", (sqr_sum / reader.len() as f64).sqrt());
}
