import requests
import pandas as pd
from datetime import datetime
import os
import logging
from typing import Dict, List, Optional
import time

class CBFDataExporter:
    def __init__(self, api_key: str):
        """
        Initialize CBF API exporter
        
        Args:
            api_key: Your CBF API key
        """
        self.base_url = "https://api.cbf.com.br/v1"
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json"
        }
        self.setup_logging()

    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_competition_id(self, year: int) -> str:
        """
        Get the competition ID for Brasileirão Serie A for a specific year
        
        Args:
            year: The year of the competition
            
        Returns:
            Competition ID
        """
        url = f"{self.base_url}/competitions"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        competitions = response.json()
        
        # Find Brasileirão Serie A for the specified year
        for comp in competitions:
            if (comp['name'].lower().find('brasileiro') != -1 and 
                comp['series'] == 'A' and 
                comp['season'] == str(year)):
                return comp['id']
                
        raise ValueError(f"Competition not found for year {year}")

    def get_matches(self, competition_id: str) -> List[Dict]:
        """
        Get all matches for a competition
        
        Args:
            competition_id: The CBF competition ID
            
        Returns:
            List of match dictionaries
        """
        url = f"{self.base_url}/competitions/{competition_id}/matches"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        matches = response.json()
        self.logger.info(f"Retrieved {len(matches)} matches")
        return matches

    def process_matches(self, matches: List[Dict]) -> pd.DataFrame:
        """
        Process matches data into a pandas DataFrame
        
        Args:
            matches: List of match dictionaries from the API
            
        Returns:
            Processed DataFrame
        """
        processed_matches = []
        
        for match in matches:
            processed_match = {
                'data': match['date'],
                'rodada': match['round'],
                'time_casa': match['home_team']['name'],
                'time_visitante': match['away_team']['name'],
                'gols_casa': match['home_score'],
                'gols_visitante': match['away_score'],
                'estadio': match['stadium']['name'],
                'cidade': match['stadium']['city'],
                'estado': match['stadium']['state'],
                'arbitro': match['referee']['name'],
                'publico': match['attendance'],
                'renda': match['revenue']
            }
            processed_matches.append(processed_match)
            
        return pd.DataFrame(processed_matches)

    def export_season(self, year: int, output_dir: str = "exports") -> str:
        """
        Export an entire season's matches to CSV
        
        Args:
            year: Season year
            output_dir: Directory to save the CSV file
            
        Returns:
            Path to the exported CSV file
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Get competition ID
            self.logger.info(f"Fetching competition ID for {year}")
            competition_id = self.get_competition_id(year)
            
            # Get matches
            self.logger.info("Fetching matches data")
            matches = self.get_matches(competition_id)
            
            # Process matches
            self.logger.info("Processing matches data")
            df = self.process_matches(matches)
            
            # Generate output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"brasileirao_{year}_{timestamp}.csv"
            filepath = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            self.logger.info(f"Successfully exported matches to {filepath}")
            
            # Log export summary
            self._log_export_summary(df)
            
            return filepath
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Export failed: {str(e)}")
            raise

    def _log_export_summary(self, df: pd.DataFrame) -> None:
        """Log summary statistics of exported data"""
        self.logger.info("\nExport Summary:")
        self.logger.info(f"Total matches: {len(df)}")
        self.logger.info(f"Date range: {df['data'].min()} to {df['data'].max()}")
        self.logger.info(f"Total teams: {len(set(df['time_casa'].unique()) | set(df['time_visitante'].unique()))}")
        self.logger.info(f"Total goals: {df['gols_casa'].sum() + df['gols_visitante'].sum()}")
        self.logger.info(f"Average attendance: {df['publico'].mean():.0f}")

def main():
    # Your CBF API key
    API_KEY = "your_cbf_api_key_here"
    
    try:
        # Initialize exporter
        exporter = CBFDataExporter(API_KEY)
        
        # Export 2023 season
        output_file = exporter.export_season(2023)
        print(f"\nExport completed successfully!")
        print(f"File saved to: {output_file}")
        
    except Exception as e:
        print(f"\nError during export: {str(e)}")

if __name__ == "__main__":
    main()
