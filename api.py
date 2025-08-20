import io
import json
import tempfile
from pathlib import Path
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from claim_pipeline import process_claims


app = FastAPI(
    title="Claim Resubmission Pipeline API",
    description="Upload EMR data files and get resubmission candidates",
    version="1.0.0",
)


@app.get("/")
async def root():
    return {"message": "Claim Resubmission Pipeline API", "version": "1.0.0"}


@app.post("/upload/alpha", response_model=List[dict])
async def upload_alpha_csv(file: UploadFile = File(...)):
    """Upload EMR Alpha CSV file and return resubmission candidates."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        content = await file.read()
        # Create temporary file for processing
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        
        # Process with pipeline
        output_path = Path("temp_candidates.json")
        metrics_path = Path("temp_metrics.json")
        
        process_claims(
            alpha_path=tmp_path,
            beta_path=Path("/dev/null"),  # No beta data for alpha-only uploads
            out_path=output_path,
            metrics_out_path=metrics_path,
            rejections_log_path=None,
        )
        
        # Read results
        with open(output_path, 'r') as f:
            candidates = json.load(f)
        
        # Cleanup temp files
        tmp_path.unlink()
        output_path.unlink()
        metrics_path.unlink()
        
        return candidates
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/upload/beta", response_model=List[dict])
async def upload_beta_json(file: UploadFile = File(...)):
    """Upload EMR Beta JSON file and return resubmission candidates."""
    if not file.filename.endswith('.json'):
        raise HTTPException(status_code=400, detail="File must be a JSON")
    
    try:
        content = await file.read()
        # Create temporary file for processing
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        
        # Process with pipeline
        output_path = Path("temp_candidates.json")
        metrics_path = Path("temp_metrics.json")
        
        process_claims(
            alpha_path=Path("/dev/null"),  # No alpha data for beta-only uploads
            beta_path=tmp_path,
            out_path=output_path,
            metrics_path=metrics_path,
            rejections_log_path=None,
        )
        
        # Read results
        with open(output_path, 'r') as f:
            candidates = json.load(f)
        
        # Cleanup temp files
        tmp_path.unlink()
        output_path.unlink()
        metrics_path.unlink()
        
        return candidates
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/upload/both", response_model=List[dict])
async def upload_both_files(
    alpha_file: UploadFile = File(...),
    beta_file: UploadFile = File(...)
):
    """Upload both EMR Alpha CSV and Beta JSON files and return resubmission candidates."""
    if not alpha_file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Alpha file must be a CSV")
    if not beta_file.filename.endswith('.json'):
        raise HTTPException(status_code=400, detail="Beta file must be a JSON")
    
    try:
        alpha_content = await alpha_file.read()
        beta_content = await beta_file.read()
        
        # Create temporary files for processing
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as alpha_tmp:
            alpha_tmp.write(alpha_content)
            alpha_path = Path(alpha_tmp.name)
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as beta_tmp:
            beta_tmp.write(beta_content)
            beta_path = Path(beta_tmp.name)
        
        # Process with pipeline
        output_path = Path("temp_candidates.json")
        metrics_path = Path("temp_metrics.json")
        
        process_claims(
            alpha_path=alpha_path,
            beta_path=beta_path,
            out_path=output_path,
            metrics_path=metrics_path,
            rejections_log_path=None,
        )
        
        # Read results
        with open(output_path, 'r') as f:
            candidates = json.load(f)
        
        # Cleanup temp files
        alpha_path.unlink()
        beta_path.unlink()
        output_path.unlink()
        metrics_path.unlink()
        
        return candidates
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
