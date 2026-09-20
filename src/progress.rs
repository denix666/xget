use indicatif::{ProgressBar, ProgressStyle};

pub fn create(total: Option<u64>) -> ProgressBar {
    match total {
        Some(total) => {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})",
                    )
                    .unwrap()
                    .progress_chars("█▓░"),
            );
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {bytes} ({bytes_per_sec})")
                    .unwrap(),
            );
            pb
        }
    }
}
