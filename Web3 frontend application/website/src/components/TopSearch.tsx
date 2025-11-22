import { Row, Col, Tooltip, OverlayTrigger } from "react-bootstrap";
import React, { useRef } from "react";
import cssStyle from "@/styles/topSearch.module.css";
import Image from "next/image";
import { useDispatch } from "react-redux";
import { AppDispatch, setWhere } from "@/store/store";

const TopSearch: React.FC = React.memo(() => {
  const dispatch = useDispatch<AppDispatch>();
  const inputRef = useRef<HTMLInputElement>(null);

  return (

      <Row className="align-items-center">
        <Col className="Col-auto me-auto d-flex">
          <OverlayTrigger placement="bottom" overlay={<Tooltip>Click to search</Tooltip>}>
            <Image
              className={cssStyle.top_find_img}
              src="/find.svg"
              width={18}
              height={18}
              alt="find"
              onClick={() => {
                if (inputRef.current) {
                  dispatch(setWhere(inputRef.current.value.trim()))                  
                }
              }}
            />
          </OverlayTrigger>
          <input style={{width:'260px'}}
            ref={inputRef}
            className={`form-control  ${cssStyle.top_find_input}`}
            placeholder='Search music by name/symbol'
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                dispatch(setWhere(e.currentTarget.value.trim()))  
              }
            }}
          />
        </Col>

      </Row>

  );
});
TopSearch.displayName="TopSearch";
export default React.memo(TopSearch);
