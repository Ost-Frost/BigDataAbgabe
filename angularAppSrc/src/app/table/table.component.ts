import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-table',
  templateUrl: './table.component.html',
  styleUrls: ['./table.component.css']
})
export class TableComponent implements OnInit {

  searchString: string = "";

  elements: any[] = []

  constructor() { }

  ngOnInit(): void {

  }

  async searchElements() {
    let response = await fetch("search?name=" + this.searchString);
    let cards = await response.json() as any[];
    this.elements = cards;
  }

}
