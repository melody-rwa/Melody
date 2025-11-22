import { Pagination } from "react-bootstrap";
import React from "react";

interface PageItemProps {
  records: number;  
  currentPageNum: number;
  pages: number; 
  setCurrentPageNum: React.Dispatch<React.SetStateAction<number>>;
  postStatus: string; 
}

const PageItem: React.FC<PageItemProps> = ({
  records,
  currentPageNum,
  pages,
  setCurrentPageNum,
  postStatus,
}) => {
  
  const tStyle: React.CSSProperties = {
    color: "#F9752D",
    fontWeight: "bold",
    padding: "0 4px",
  };

  const isLoading = postStatus !== "succeeded";

  return (
    <>
      {pages > 1 && (
        <div className="px-3" >
          <div className="d-flex align-items-center justify-content-between p-2">
            <div style={{color:'#FFF',opacity:0.8}} >Records：<span style={tStyle}>{records}</span></div>
            <div style={{color:'#FFF',opacity:0.8}}>Current：<span style={tStyle}>{currentPageNum}</span></div>
            <div style={{color:'#FFF',opacity:0.8}}>pages：<span style={tStyle}>{pages}</span></div>
          </div>
          <div className="d-flex align-item-center justify-content-center">
            <Pagination >
              {pages > 2 && (
                <Pagination.First
                  disabled={isLoading || currentPageNum === 1}
                  onClick={() => setCurrentPageNum(1)}
                />
              )}
              <Pagination.Prev
                disabled={isLoading || currentPageNum === 1}
                onClick={() => setCurrentPageNum(currentPageNum - 1)}
              />
              <Pagination.Next
                disabled={isLoading || currentPageNum === pages}
                onClick={() => setCurrentPageNum(currentPageNum + 1)}
              />
              {pages > 2 && (
                <Pagination.Last
                  disabled={isLoading || currentPageNum === pages}
                  onClick={() => setCurrentPageNum(pages)}
                />
              )}
            </Pagination>
          </div>
        </div>
      )}
    </>
  );
};

export default PageItem;
