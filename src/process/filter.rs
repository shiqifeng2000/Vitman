use webrtc_audio_processing::{
    AudioProcessing, Config, EchoCancellation, EchoCancellationSuppressionLevel,
    InitializationConfig, NoiseSuppression, Processor,
};

pub struct AudioProcessor {
    processor: AudioProcessing,
    // stream_config: StreamConfig,
}

impl AudioProcessor {
    pub fn new(sample_rate: u32, channels: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let mut processor = Processor::new(&InitializationConfig {
            num_capture_channels: channels as i32,
            num_render_channels: channels as i32,
            ..Default::default()
        })?;
        let config = Config {
            echo_cancellation: Some(EchoCancellation {
                suppression_level: EchoCancellationSuppressionLevel::High,
                enable_delay_agnostic: false,
                enable_extended_filter: false,
                stream_delay_ms: None,
            }),
            noise_suppression: Some(NoiseSuppression {
                suppression_level: webrtc_audio_processing::NoiseSuppressionLevel::Moderate,
            }),
            ..Default::default()
        };
        processor.set_config(config);

        processor.process_capture_frame(frame)
        processor.process_render_frame(frame)

        let processor = AudioProcessing::new(config)?;

        let stream_config = StreamConfig::new(sample_rate, channels);

        Ok(AudioProcessor {
            processor,
            stream_config,
        })
    }

    /// 处理远端音频（建立参考信号）
    pub fn process_reverse_stream(
        &mut self,
        audio_data: &[i16],
    ) -> Result<Vec<i16>, Box<dyn std::error::Error>> {
        self.processor
            .process_reverse_stream(audio_data, self.stream_config)?;
        Ok(audio_data.to_vec())
    }

    /// 处理近端音频（应用回声消除和噪音抑制）
    pub fn process_stream(
        &mut self,
        audio_data: &[i16],
    ) -> Result<Vec<i16>, Box<dyn std::error::Error>> {
        let mut processed = vec![0i16; audio_data.len()];
        self.processor
            .process_stream(audio_data, &mut processed, self.stream_config)?;
        Ok(processed)
    }
}
